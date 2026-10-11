// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Tests of cancellation through the driver manager, focusing on making sure
// cancellation is truly concurrent.

use std::cell::RefCell;
use std::ffi::{c_int, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering::SeqCst};
use std::thread;
use std::time::{Duration, Instant};

use adbc_core::error::{AdbcStatusCode, Result, Status};
use adbc_core::options::AdbcVersion;
use adbc_core::{CancelHandle, Connection, Database, Driver, Statement};
use adbc_driver_manager::{ManagedConnection, ManagedDriver, ManagedStatement};
use adbc_ffi::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError,
    FFI_AdbcStatement,
};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

mod common;

use common::AtomicFlag;

const TIMEOUT: Duration = Duration::from_secs(2);
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(10);

// ------------------------------------------------------------
// Simulated driver implementation
// ------------------------------------------------------------

// Track what's been done with the driver so we can verify invariants later.
struct DriverObjectState {
    // count of non-cancel operations
    operations: AtomicUsize,
    // count of cancel operations
    cancellations: AtomicUsize,
    // allow the operation to proceed
    release_operation: AtomicFlag,
    // allow the cancel to proceed
    release_cancel: AtomicFlag,
    // status to return from the cancel callback
    cancel_status: AtomicU8,
    // whether the operation or cancel timed out waiting for the other to release
    timed_out: AtomicBool,
    statement_releases: AtomicUsize,
    connection_releases: AtomicUsize,
    database_releases: AtomicUsize,
    driver_releases: AtomicUsize,
}

impl DriverObjectState {
    fn new() -> Self {
        Self {
            operations: AtomicUsize::new(0),
            cancellations: AtomicUsize::new(0),
            release_operation: AtomicFlag::new(false),
            release_cancel: AtomicFlag::new(true),
            cancel_status: AtomicU8::new(Status::Ok.into()),
            timed_out: AtomicBool::new(false),
            statement_releases: AtomicUsize::new(0),
            connection_releases: AtomicUsize::new(0),
            database_releases: AtomicUsize::new(0),
            driver_releases: AtomicUsize::new(0),
        }
    }

    // Do "something" with the driver, waiting on release_operation
    fn operate(&self) -> AdbcStatusCode {
        self.operations.fetch_add(1, SeqCst);
        if !self.release_operation.wait(CALLBACK_TIMEOUT) {
            self.timed_out.store(true, SeqCst);
            return Status::Internal.into();
        }
        Status::Cancelled.into()
    }

    // Cancel operate() by flagging release_operation, waiting on release_cancel first
    fn cancel(&self) -> AdbcStatusCode {
        self.cancellations.fetch_add(1, SeqCst);
        if !self.release_cancel.wait(CALLBACK_TIMEOUT) {
            self.timed_out.store(true, SeqCst);
            return Status::Internal.into();
        }
        self.release_operation.set();
        self.cancel_status.load(SeqCst)
    }

    fn releases(&self) -> [usize; 4] {
        [
            self.statement_releases.load(SeqCst),
            self.connection_releases.load(SeqCst),
            self.database_releases.load(SeqCst),
            self.driver_releases.load(SeqCst),
        ]
    }

    // SAFETY: Each private_data is a Box<Arc<State>> initialized below. Callbacks
    // clone the Arc before blocking and never borrow the mutable FFI handle itself.
    unsafe fn from_raw(data: *mut c_void) -> Arc<Self> {
        unsafe { Arc::clone(&*data.cast::<Arc<Self>>()) }
    }
}

fn wait_for(condition: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !condition() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    condition()
}

thread_local! {
    // We need a way to pass state to the driver impl, so use a thread local.
    static INITIAL_STATE: RefCell<Option<Arc<DriverObjectState>>> = const { RefCell::new(None) };
}

// ------------------------------------------------------------
// Simulated driver implementation (driver callbacks)
// ------------------------------------------------------------

macro_rules! release_callback {
    ($name:ident, $ty:ty, $counter:ident) => {
        unsafe extern "C" fn $name(handle: *mut $ty, _: *mut FFI_AdbcError) -> AdbcStatusCode {
            unsafe {
                if (*handle).private_data.is_null() {
                    return Status::InvalidState.into();
                }
                let state = Box::from_raw((*handle).private_data.cast::<Arc<DriverObjectState>>());
                state.$counter.fetch_add(1, SeqCst);
                (*handle).private_data = std::ptr::null_mut();
            }
            Status::Ok.into()
        }
    };
}

release_callback!(driver_release, FFI_AdbcDriver, driver_releases);
release_callback!(database_release, FFI_AdbcDatabase, database_releases);
release_callback!(connection_release, FFI_AdbcConnection, connection_releases);
release_callback!(statement_release, FFI_AdbcStatement, statement_releases);

unsafe extern "C" fn database_new(
    database: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe {
        let state = DriverObjectState::from_raw((*(*database).private_driver).private_data);
        (*database).private_data = Box::into_raw(Box::new(state)).cast();
    }
    Status::Ok.into()
}

unsafe extern "C" fn database_init(
    _: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    Status::Ok.into()
}

unsafe extern "C" fn connection_new(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    Status::Ok.into()
}

unsafe extern "C" fn connection_init(
    connection: *mut FFI_AdbcConnection,
    database: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe {
        (*connection).private_data = Box::into_raw(Box::new(DriverObjectState::from_raw(
            (*database).private_data,
        )))
        .cast();
    }
    Status::Ok.into()
}

unsafe extern "C" fn statement_new(
    connection: *mut FFI_AdbcConnection,
    statement: *mut FFI_AdbcStatement,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe {
        (*statement).private_data = Box::into_raw(Box::new(DriverObjectState::from_raw(
            (*connection).private_data,
        )))
        .cast();
    }
    Status::Ok.into()
}

unsafe extern "C" fn commit(
    connection: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe { DriverObjectState::from_raw((*connection).private_data).operate() }
}

unsafe extern "C" fn execute(
    statement: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowArrayStream,
    _: *mut i64,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe { DriverObjectState::from_raw((*statement).private_data).operate() }
}

unsafe extern "C" fn connection_cancel(
    handle: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe { DriverObjectState::from_raw((*handle).private_data).cancel() }
}

unsafe extern "C" fn statement_cancel(
    handle: *mut FFI_AdbcStatement,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe { DriverObjectState::from_raw((*handle).private_data).cancel() }
}

unsafe extern "C" fn init<const CANCEL: bool>(
    _: c_int,
    driver: *mut c_void,
    _: *mut FFI_AdbcError,
) -> AdbcStatusCode {
    let Some(state) = INITIAL_STATE.with(|state| state.borrow_mut().take()) else {
        return Status::Internal.into();
    };
    unsafe {
        *driver.cast::<FFI_AdbcDriver>() = FFI_AdbcDriver {
            private_data: Box::into_raw(Box::new(state)).cast(),
            release: Some(driver_release),
            DatabaseNew: Some(database_new),
            DatabaseInit: Some(database_init),
            DatabaseRelease: Some(database_release),
            ConnectionNew: Some(connection_new),
            ConnectionInit: Some(connection_init),
            ConnectionRelease: Some(connection_release),
            ConnectionCommit: Some(commit),
            ConnectionCancel: if CANCEL {
                Some(connection_cancel)
            } else {
                None
            },
            StatementNew: Some(statement_new),
            StatementRelease: Some(statement_release),
            StatementExecuteQuery: Some(execute),
            StatementCancel: if CANCEL { Some(statement_cancel) } else { None },
            ..FFI_AdbcDriver::default()
        };
    }
    Status::Ok.into()
}

// ------------------------------------------------------------
// Test cases
// ------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum ObjectKind {
    Connection,
    Statement,
}

const OBJECT_KINDS: [ObjectKind; 2] = [ObjectKind::Connection, ObjectKind::Statement];

#[derive(Clone)]
enum Object {
    Connection(ManagedConnection),
    Statement(ManagedStatement),
}

impl Object {
    fn cancel_handle(&self) -> Box<dyn CancelHandle> {
        match self {
            Self::Connection(connection) => connection.get_cancel_handle(),
            Self::Statement(statement) => statement.get_cancel_handle(),
        }
    }

    fn operate(&mut self) -> Result<()> {
        match self {
            Self::Connection(connection) => connection.commit(),
            Self::Statement(statement) => statement.execute().map(|_| ()),
        }
    }
}

fn setup<const CANCEL: bool>(
    kind: ObjectKind,
    version: AdbcVersion,
) -> Result<(Arc<DriverObjectState>, Object)> {
    let state = Arc::new(DriverObjectState::new());
    INITIAL_STATE.with(|slot| *slot.borrow_mut() = Some(state.clone()));
    let mut driver =
        ManagedDriver::load_static(&(init::<CANCEL> as FFI_AdbcDriverInitFunc), version)?;
    let database = driver.new_database()?;
    let mut connection = database.new_connection()?;
    let object = match kind {
        ObjectKind::Connection => Object::Connection(connection),
        ObjectKind::Statement => Object::Statement(connection.new_statement()?),
    };
    Ok((state, object))
}

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[test]
fn cancel_is_concurrent() -> TestResult {
    for kind in OBJECT_KINDS {
        let (state, mut object) = setup::<true>(kind, AdbcVersion::V110)?;
        let cancel = object.cancel_handle();
        let execution = thread::spawn(move || (object.operate(), object));
        let entered = wait_for(|| state.operations.load(SeqCst) == 1, TIMEOUT);
        let cancellation = thread::spawn(move || cancel.try_cancel());
        let overlapped = wait_for(|| state.cancellations.load(SeqCst) == 1, TIMEOUT);
        state.release_operation.set(); // in case cancel didn't work, unblock
        let (result, object) = execution.join().map_err(|_| "worker thread panicked")?;
        cancellation
            .join()
            .map_err(|_| "worker thread panicked")??;
        assert!(entered, "{kind:?} did not enter the driver");
        assert!(overlapped, "{kind:?} blocked cancellation");
        assert_eq!(
            result.err().map(|error| error.status),
            Some(Status::Cancelled)
        );
        assert!(!state.timed_out.load(SeqCst));
        drop(object);
    }
    Ok(())
}

#[test]
fn cancellation_keeps_native_handles_alive() -> TestResult {
    for kind in OBJECT_KINDS {
        let (state, object) = setup::<true>(kind, AdbcVersion::V110)?;
        let clone = object.clone();
        drop(clone);
        assert_eq!(
            state.releases(),
            [0; 4],
            "dropping a clone released {kind:?}"
        );
        state.release_cancel.clear();
        let cancel = object.cancel_handle();
        let expired = object.cancel_handle();
        let cancellation = thread::spawn(move || cancel.try_cancel());
        let entered = wait_for(|| state.cancellations.load(SeqCst) == 1, TIMEOUT);
        drop(object);
        let releases_during_cancel = state.releases();
        state.release_cancel.set();
        cancellation
            .join()
            .map_err(|_| "worker thread panicked")??;
        assert!(entered);
        assert_eq!(releases_during_cancel, [0; 4]);
        let expected = match kind {
            ObjectKind::Connection => [0, 1, 1, 1],
            ObjectKind::Statement => [1, 1, 1, 1],
        };
        assert_eq!(state.releases(), expected);
        assert!(!state.timed_out.load(SeqCst));
        expired.try_cancel()?;
        assert_eq!(state.cancellations.load(SeqCst), 1);
    }
    Ok(())
}

#[test]
fn concurrent_cancellation_and_callback_errors() -> TestResult {
    for kind in OBJECT_KINDS {
        let (state, object) = setup::<true>(kind, AdbcVersion::V110)?;
        state.release_cancel.clear();
        state
            .cancel_status
            .store(Status::InvalidState.into(), SeqCst);
        let cancel: Arc<dyn CancelHandle> = object.cancel_handle().into();
        let second_cancel = cancel.clone();
        let first = thread::spawn(move || cancel.try_cancel());
        let second = thread::spawn(move || second_cancel.try_cancel());
        let overlapped = wait_for(|| state.cancellations.load(SeqCst) == 2, TIMEOUT);
        state.release_cancel.set();
        let results = [
            first.join().map_err(|_| "worker thread panicked")?,
            second.join().map_err(|_| "worker thread panicked")?,
        ];
        assert!(overlapped, "{kind:?} cancellation calls did not overlap");
        for result in results {
            assert_eq!(
                result.err().map(|error| error.status),
                Some(Status::InvalidState)
            );
        }
        assert!(!state.timed_out.load(SeqCst));
    }
    Ok(())
}

fn unsupported<const CANCEL: bool>(kind: ObjectKind, version: AdbcVersion) -> TestResult {
    let (state, mut object) = setup::<CANCEL>(kind, version)?;
    let cancel = object.cancel_handle();
    let execution = thread::spawn(move || (object.operate(), object));
    let entered = wait_for(|| state.operations.load(SeqCst) == 1, TIMEOUT);
    let finished = Arc::new(AtomicBool::new(false));
    let cancel_finished = finished.clone();
    let cancellation = thread::spawn(move || {
        let result = cancel.try_cancel();
        cancel_finished.store(true, SeqCst);
        result
    });
    let returned = wait_for(|| finished.load(SeqCst), TIMEOUT);
    state.release_operation.set();
    let (result, object) = execution.join().map_err(|_| "worker thread panicked")?;
    let cancel_result = cancellation.join().map_err(|_| "worker thread panicked")?;
    assert!(
        entered && returned,
        "unsupported {kind:?} cancellation blocked"
    );
    assert_eq!(
        cancel_result.err().map(|error| error.status),
        Some(Status::NotImplemented)
    );
    assert_eq!(
        result.err().map(|error| error.status),
        Some(Status::Cancelled)
    );
    assert_eq!(state.cancellations.load(SeqCst), 0);
    assert!(!state.timed_out.load(SeqCst));
    let expired = object.cancel_handle();
    drop(object);
    expired.try_cancel()?;
    Ok(())
}

#[test]
fn unsupported_cancellation_does_not_wait() -> TestResult {
    for kind in OBJECT_KINDS {
        unsupported::<false>(kind, AdbcVersion::V110)?;
        unsupported::<true>(kind, AdbcVersion::V100)?;
    }
    Ok(())
}
