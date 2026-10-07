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

use std::sync::{Arc, Mutex};

use adbc_core::error::{Result, Status};
use adbc_core::options::AdbcVersion;
use adbc_core::{CancelHandle, Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;
use adbc_ffi::FFI_AdbcDriverInitFunc;
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
