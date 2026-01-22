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

//! Snowflake ADBC Statement
//!
//!

use adbc_core::{
    error::Result,
    options::{OptionStatement, OptionValue},
    Optionable, PartitionedResult,
};
use adbc_driver_manager::ManagedStatement;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

/// Snowflake ADBC Statement.
pub struct Statement(pub(crate) ManagedStatement);

impl Optionable for Statement {
    type Option = OptionStatement;

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

impl adbc_core::Statement for Statement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.0.bind(batch)
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.0.bind_stream(reader)
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.0.execute()
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.0.execute_update()
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        self.0.execute_schema()
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        self.0.execute_partitions()
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        self.0.get_parameter_schema()
    }

    fn prepare(&mut self) -> Result<()> {
        self.0.prepare()
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.0.set_sql_query(query)
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        self.0.set_substrait_plan(plan)
    }

    fn get_cancel_handle(&self) -> Box<dyn adbc_core::CancelHandle> {
        self.0.get_cancel_handle()
    }
}
