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

//! Snowflake ADBC Connection
//!
//!

use std::collections::HashSet;

use adbc_core::{
    error::Result,
    options::{InfoCode, OptionConnection, OptionValue},
    Optionable,
};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

use crate::Statement;

mod builder;
pub use builder::*;

/// Snowflake ADBC Connection.
#[derive(Clone)]
pub struct Connection(pub(crate) ManagedConnection);

impl Optionable for Connection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.0.set_option(key, value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.0.get_option_string(key)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.0.get_option_bytes(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.0.get_option_int(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.0.get_option_double(key)
    }
}

impl adbc_core::Connection for Connection {
    type StatementType = Statement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        self.0.new_statement().map(Statement)
    }

    fn get_cancel_handle(&self) -> Box<dyn adbc_core::CancelHandle> {
        self.0.get_cancel_handle()
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0.get_info(codes)
    }

    fn get_objects(
        &self,
        depth: adbc_core::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0.get_objects(
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.0.get_table_schema(catalog, db_schema, table_name)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0.get_table_types()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0.get_statistic_names()
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0
            .get_statistics(catalog, db_schema, table_name, approximate)
    }

    fn commit(&mut self) -> Result<()> {
        self.0.commit()
    }

    fn rollback(&mut self) -> Result<()> {
        self.0.rollback()
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0.read_partition(partition)
    }
}
