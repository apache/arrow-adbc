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

// Test that the driver exporter and cancellation do not have undefined
// behavior by introducing overlapping mutable borrows. Run under Miri.

use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use adbc_core::constants::ADBC_STATUS_OK;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::AdbcVersion;
use adbc_core::{CancelHandle, Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;
use adbc_ffi::{FFI_AdbcDatabase, FFI_AdbcDriverInitFunc, FFI_AdbcError, FFIDriver};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

mod common;

struct CancelOnDropReader {
    cancel: Box<dyn CancelHandle>,
    status: Arc<Mutex<Option<Status>>>,
}

impl Iterator for CancelOnDropReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for CancelOnDropReader {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

impl Drop for CancelOnDropReader {
    fn drop(&mut self) {
        let status = match self.cancel.try_cancel() {
            Ok(()) => Status::Ok,
            Err(error) => error.status,
        };
        if let Ok(mut recorded) = self.status.lock() {
            *recorded = Some(status);
        }
    }
}

#[test]
fn statement_cancellation() -> Result<()> {
    let mut driver = ManagedDriver::load_static(
        &(common::dummy::AdbcDummyInit as FFI_AdbcDriverInitFunc),
        AdbcVersion::V110,
    )?;
    let database = driver.new_database()?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    let status = Arc::new(Mutex::new(None));
    let reader = CancelOnDropReader {
        cancel: statement.get_cancel_handle(),
        status: status.clone(),
    };

    // The dummy driver drops the reader while bind_stream's mutable statement
    // borrow is live. Its destructor re-enters statement_private_data through
    // cancellation, creating a potential overlapping mutable borrow of the
    // exporter state. This was fixed, but this ensures this case continues to
    // work and should be run via miri.
    statement.bind_stream(Box::new(reader))?;
    assert_eq!(*status.lock().unwrap(), Some(Status::Unknown));
    Ok(())
}

#[test]
fn connection_cancellation() -> Result<()> {
    let mut driver = ManagedDriver::load_static(
        &(common::dummy::AdbcDummyInit as FFI_AdbcDriverInitFunc),
        AdbcVersion::V110,
    )?;
    let database = driver.new_database()?;
    let mut connection = database.new_connection()?;

    let handle = connection.get_cancel_handle();
    let guard = std::thread::spawn(move || {
        assert!(common::dummy::IN_COMMIT.wait(std::time::Duration::from_secs(5)));
        handle.try_cancel().unwrap();
    });

    connection.commit()?;
    guard.join().unwrap();
    Ok(())
}

// Exercise database getters directly (avoiding driver manager's lock) to test
// for potential UB in the exporter under concurrent access.
fn concurrent_database_getters(initialized: bool) -> Result<()> {
    let driver = common::dummy::DummyDriver::ffi_driver();
    let new = driver.DatabaseNew.unwrap();
    let init = driver.DatabaseInit.unwrap();
    let release = driver.DatabaseRelease.unwrap();
    let set_string = driver.DatabaseSetOption.unwrap();
    let set_bytes = driver.DatabaseSetOptionBytes.unwrap();
    let set_int = driver.DatabaseSetOptionInt.unwrap();
    let set_double = driver.DatabaseSetOptionDouble.unwrap();
    let get_string = driver.DatabaseGetOption.unwrap();
    let get_bytes = driver.DatabaseGetOptionBytes.unwrap();
    let get_int = driver.DatabaseGetOptionInt.unwrap();
    let get_double = driver.DatabaseGetOptionDouble.unwrap();

    let mut database = FFI_AdbcDatabase::default();
    let mut error = FFI_AdbcError::default();
    // Use the same raw pointer throughout: workers must not create &mut borrows
    // of the shared handle, and passing through AtomicPtr preserves provenance.
    let database_ptr = &raw mut database;
    unsafe {
        assert_eq!(new(database_ptr, &mut error), ADBC_STATUS_OK);
        assert_eq!(
            set_string(
                database_ptr,
                c"string".as_ptr(),
                c"hello".as_ptr(),
                &mut error
            ),
            ADBC_STATUS_OK
        );
        assert_eq!(
            set_bytes(
                database_ptr,
                c"bytes".as_ptr(),
                b"hello".as_ptr(),
                5,
                &mut error
            ),
            ADBC_STATUS_OK
        );
        assert_eq!(
            set_int(database_ptr, c"int".as_ptr(), 42, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            set_double(database_ptr, c"double".as_ptr(), 1.25, &mut error),
            ADBC_STATUS_OK
        );
        if initialized {
            assert_eq!(init(database_ptr, &mut error), ADBC_STATUS_OK);
            // Also exercise the setter against the initialized database mutex.
            assert_eq!(
                set_int(database_ptr, c"int".as_ptr(), 42, &mut error),
                ADBC_STATUS_OK
            );
        }
    }

    let shared = AtomicPtr::new(database_ptr);
    let barrier = Barrier::new(2);
    let result = std::thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::new();
        for _ in 0..2 {
            workers.push(scope.spawn(|| {
                let database = shared.load(Ordering::Relaxed);
                let mut error = FFI_AdbcError::default();
                barrier.wait();
                for _ in 0..4 {
                    let mut string = [0; 6];
                    let mut length = string.len();
                    unsafe {
                        assert_eq!(
                            get_string(
                                database,
                                c"string".as_ptr(),
                                string.as_mut_ptr().cast(),
                                &mut length,
                                &mut error
                            ),
                            ADBC_STATUS_OK
                        );
                    }
                    assert_eq!(length, 6);
                    assert_eq!(&string, b"hello\0");

                    let mut bytes = [0; 5];
                    let mut length = bytes.len();
                    unsafe {
                        assert_eq!(
                            get_bytes(
                                database,
                                c"bytes".as_ptr(),
                                bytes.as_mut_ptr(),
                                &mut length,
                                &mut error
                            ),
                            ADBC_STATUS_OK
                        );
                    }
                    assert_eq!(length, 5);
                    assert_eq!(&bytes, b"hello");

                    let mut int = 0;
                    unsafe {
                        assert_eq!(
                            get_int(database, c"int".as_ptr(), &mut int, &mut error),
                            ADBC_STATUS_OK
                        );
                    }
                    assert_eq!(int, 42);

                    let mut double = 0.0;
                    unsafe {
                        assert_eq!(
                            get_double(database, c"double".as_ptr(), &mut double, &mut error),
                            ADBC_STATUS_OK
                        );
                    }
                    assert_eq!(double, 1.25);
                }
            }));
        }
        for worker in workers {
            worker.join().map_err(|_| {
                Error::with_message_and_status("Database getter worker panicked", Status::Internal)
            })?;
        }
        Ok(())
    });

    unsafe {
        assert_eq!(release(database_ptr, &mut error), ADBC_STATUS_OK);
    }
    result
}

#[test]
fn database_getters_before_init() -> Result<()> {
    concurrent_database_getters(false)
}

#[test]
fn database_getters_after_init() -> Result<()> {
    concurrent_database_getters(true)
}
