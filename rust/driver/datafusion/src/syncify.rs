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
#![allow(dead_code)]
use std::sync::Arc;

use adbc_core::{
    non_blocking::{AsyncConnection, AsyncDatabase, AsyncDriver, AsyncOptionable, AsyncStatement},
    options::{OptionConnection, OptionDatabase, OptionStatement},
    Connection, Database, Driver, Optionable, Statement,
};
use tokio::runtime::Runtime;

use crate::{DataFusionConnection, DataFusionDatabase, DataFusionDriver, DataFusionStatement};

pub struct SyncDataFusionDriver {
    driver: DataFusionDriver,
    rt: Arc<Runtime>,
}

pub struct SyncDataFusionDatabase {
    db: DataFusionDatabase,
    rt: Arc<Runtime>,
}

pub struct SyncDataFusionConnection {
    conn: DataFusionConnection,
    rt: Arc<Runtime>,
}

pub struct SyncDataFusionStatement {
    stmt: DataFusionStatement,
    rt: Arc<Runtime>,
}

impl Default for SyncDataFusionDriver {
    fn default() -> Self {
        Self {
            driver: Default::default(),
            rt: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
        }
    }
}

impl Driver for SyncDataFusionDriver {
    type DatabaseType = SyncDataFusionDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        let db = self.rt.block_on(self.driver.new_database())?;

        Ok(SyncDataFusionDatabase {
            db,
            rt: self.rt.clone(),
        })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionDatabase,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let db = self.rt.block_on(
            self.driver
                .new_database_with_opts(opts.into_iter().collect()),
        )?;

        Ok(SyncDataFusionDatabase {
            db,
            rt: self.rt.clone(),
        })
    }
}

impl Optionable for SyncDataFusionDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.db.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        self.rt.block_on(self.db.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        self.rt.block_on(self.db.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        self.rt.block_on(self.db.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        self.rt.block_on(self.db.get_option_double(key))
    }
}

impl Database for SyncDataFusionDatabase {
    type ConnectionType = SyncDataFusionConnection;

    fn new_connection(&self) -> adbc_core::error::Result<Self::ConnectionType> {
        let conn = self.rt.block_on(self.db.new_connection())?;

        Ok(SyncDataFusionConnection {
            conn,
            rt: self.rt.clone(),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionConnection,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        let conn = self
            .rt
            .block_on(self.db.new_connection_with_opts(opts.into_iter().collect()))?;

        Ok(SyncDataFusionConnection {
            conn,
            rt: self.rt.clone(),
        })
    }
}

impl Optionable for SyncDataFusionConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.conn.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        self.rt.block_on(self.conn.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        self.rt.block_on(self.conn.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        self.rt.block_on(self.conn.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        self.rt.block_on(self.conn.get_option_double(key))
    }
}

impl Connection for SyncDataFusionConnection {
    type StatementType = SyncDataFusionStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        let stmt = self.rt.block_on(self.conn.new_statement())?;

        Ok(SyncDataFusionStatement {
            stmt,
            rt: self.rt.clone(),
        })
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.conn.cancel())
    }

    fn get_info(
        &self,
        codes: Option<std::collections::HashSet<adbc_core::options::InfoCode>>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt.block_on(self.conn.get_info(codes))
    }

    fn get_objects(
        &self,
        depth: adbc_core::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt.block_on(self.conn.get_objects(
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> adbc_core::error::Result<arrow_schema::Schema> {
        self.rt
            .block_on(self.conn.get_table_schema(catalog, db_schema, table_name))
    }

    fn get_table_types(
        &self,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt.block_on(self.conn.get_table_types())
    }

    fn get_statistic_names(
        &self,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt.block_on(self.conn.get_statistic_names())
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt.block_on(
            self.conn
                .get_statistics(catalog, db_schema, table_name, approximate),
        )
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.conn.commit())
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.conn.rollback())
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt
            .block_on(self.conn.read_partition(partition.as_ref()))
    }
}

impl Optionable for SyncDataFusionStatement {
    type Option = OptionStatement;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.stmt.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        self.rt.block_on(self.stmt.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        self.rt.block_on(self.stmt.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        self.rt.block_on(self.stmt.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        self.rt.block_on(self.stmt.get_option_double(key))
    }
}

impl Statement for SyncDataFusionStatement {
    fn bind(&mut self, batch: arrow_array::RecordBatch) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.stmt.bind(batch))
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.stmt.bind_stream(reader))
    }

    fn execute(&mut self) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        self.rt.block_on(self.stmt.execute())
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        self.rt.block_on(self.stmt.execute_update())
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<arrow_schema::Schema> {
        self.rt.block_on(self.stmt.execute_schema())
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<adbc_core::PartitionedResult> {
        self.rt.block_on(self.stmt.execute_partitions())
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<arrow_schema::Schema> {
        self.rt.block_on(self.stmt.get_parameter_schema())
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.stmt.prepare())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.stmt.set_sql_query(query.as_ref()))
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        self.rt
            .block_on(self.stmt.set_substrait_plan(plan.as_ref()))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        self.rt.block_on(self.stmt.cancel())
    }
}
