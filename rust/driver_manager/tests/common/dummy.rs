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

use std::collections::HashMap;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};

use super::AtomicFlag;

// hackery for test
pub static IN_COMMIT: std::sync::LazyLock<AtomicFlag> =
    std::sync::LazyLock::new(|| AtomicFlag::new(false));

#[derive(Default)]
pub struct DummyDriver {}

impl Driver for DummyDriver {
    type DatabaseType = DummyDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

#[derive(Default)]
pub struct DummyDatabase {
    options: HashMap<OptionDatabase, OptionValue>,
}

impl DummyDatabase {
    fn option(&self, key: OptionDatabase) -> Result<&OptionValue> {
        // Let concurrent exported getters overlap while their driver borrows are live.
        std::thread::yield_now();
        self.options.get(&key).ok_or_else(|| {
            Error::with_message_and_status(
                format!("Option key not found: {key:?}"),
                Status::NotFound,
            )
        })
    }
}

impl Optionable for DummyDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.options.insert(key, value);
        Ok(())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        match self.option(key)? {
            OptionValue::Bytes(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                "Expected bytes option",
                Status::InvalidState,
            )),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match self.option(key)? {
            OptionValue::Double(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                "Expected double option",
                Status::InvalidState,
            )),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match self.option(key)? {
            OptionValue::Int(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                "Expected integer option",
                Status::InvalidState,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match self.option(key)? {
            OptionValue::String(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                "Expected string option",
                Status::InvalidState,
            )),
        }
    }
}

impl Database for DummyDatabase {
    type ConnectionType = DummyConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(Self::ConnectionType {
            flag: std::sync::Arc::new(AtomicFlag::new(false)),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = Self::ConnectionType {
            flag: std::sync::Arc::new(AtomicFlag::new(false)),
        };
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

pub struct DummyConnection {
    flag: std::sync::Arc<AtomicFlag>,
}

impl Optionable for DummyConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        Ok(())
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        todo!()
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        todo!()
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        todo!()
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        todo!()
    }
}

impl Connection for DummyConnection {
    type StatementType = DummyStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(Self::StatementType::default())
    }

    fn cancel(&mut self) -> Result<()> {
        todo!()
    }

    fn get_cancel_handle(&self) -> Box<dyn adbc_core::CancelHandle> {
        struct CancelHandle {
            flag: std::sync::Arc<AtomicFlag>,
        }

        impl adbc_core::CancelHandle for CancelHandle {
            fn try_cancel(&self) -> Result<()> {
                self.flag.set();
                Ok(())
            }
        }

        Box::new(CancelHandle {
            flag: self.flag.clone(),
        })
    }

    fn commit(&mut self) -> Result<()> {
        IN_COMMIT.set();

        if self.flag.wait(std::time::Duration::from_secs(5)) {
            // properly cancelled
            Ok(())
        } else {
            Err(Error::with_message_and_status(
                "Commit was not cancelled",
                Status::Internal,
            ))
        }
    }

    fn get_info(
        &self,
        _codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        todo!()
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct DummyStatement {
    // make sure this struct isn't zero-sized
    _state: i32,
}

impl Optionable for DummyStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        Ok(())
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        todo!()
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        todo!()
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        todo!()
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        todo!()
    }
}

impl Statement for DummyStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Ok(())
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        todo!()
    }

    fn execute_schema(&mut self) -> Result<arrow_schema::Schema> {
        todo!()
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Ok(Some(0))
    }

    fn get_parameter_schema(&self) -> Result<arrow_schema::Schema> {
        todo!()
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Ok(())
    }
}

adbc_ffi::export_driver!(AdbcDummyInit, DummyDriver);
