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

use std::{collections::HashSet, fmt::Debug, future::Future, pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{Schema, SchemaRef};
use futures::StreamExt;

use crate::{
    error::Result,
    non_blocking::{
        LocalAsyncConnection, LocalAsyncDatabase, LocalAsyncDriver, LocalAsyncOptionable,
        LocalAsyncStatement,
    },
    options::{
        InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    },
    Connection, Database, Driver, Optionable, Statement,
};

pub trait AsyncExecutor: Sized + Send + Sync {
    type Config: Default;
    type Error: Debug;

    fn new(config: Self::Config) -> std::result::Result<Self, Self::Error>;
    fn block_on<F: Future>(&self, future: F) -> F::Output;
}

pub struct SyncRecordBatchStream<A> {
    executor: Arc<A>,
    inner: Pin<Box<dyn crate::non_blocking::RecordBatchStream + Send>>,
}

pub struct SyncDriverWrapper<A, D> {
    inner: D,
    executor: Arc<A>,
}

impl<A: AsyncExecutor> Iterator for SyncRecordBatchStream<A> {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.executor.block_on(self.inner.next())
    }
}

impl<A: AsyncExecutor> RecordBatchReader for SyncRecordBatchStream<A> {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

pub struct WrapperRecordBatchReader {
    inner: Box<dyn RecordBatchReader + Send>,
}

impl futures::Stream for WrapperRecordBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.get_mut().inner.next())
    }
}

impl crate::non_blocking::RecordBatchStream for WrapperRecordBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }
}

impl<A: AsyncExecutor, D: Default> Default for SyncDriverWrapper<A, D> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            executor: Arc::new(A::new(Default::default()).expect("failed to create executor")),
        }
    }
}

impl<A, D> Driver for SyncDriverWrapper<A, D>
where
    A: AsyncExecutor,
    D: LocalAsyncDriver,
    D::DatabaseType: LocalAsyncDatabase + LocalAsyncOptionable<Option = OptionDatabase>,
    <D::DatabaseType as LocalAsyncDatabase>::ConnectionType:
        LocalAsyncConnection + LocalAsyncOptionable<Option = OptionConnection>,
    <<D::DatabaseType as LocalAsyncDatabase>::ConnectionType as LocalAsyncConnection>::StatementType: LocalAsyncOptionable<Option = OptionStatement>,
{
    type DatabaseType = SyncDatabaseWrapper<A, D::DatabaseType>;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        let db = self.executor.block_on(self.inner.new_database())?;

        Ok(SyncDatabaseWrapper { inner: db, executor: self.executor.clone() })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let db = self.executor.block_on(self.inner.new_database_with_opts(opts.into_iter().collect()))?;

        Ok(SyncDatabaseWrapper { inner: db, executor: self.executor.clone() })
    }
}

pub struct SyncDatabaseWrapper<A, DB> {
    inner: DB,
    executor: Arc<A>,
}

impl<A, DB> Optionable for SyncDatabaseWrapper<A, DB>
where
    A: AsyncExecutor,
    DB: LocalAsyncOptionable<Option = OptionDatabase>,
{
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.executor.block_on(self.inner.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.executor.block_on(self.inner.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.executor.block_on(self.inner.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.executor.block_on(self.inner.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.executor.block_on(self.inner.get_option_double(key))
    }
}

impl<A, DB> Database for SyncDatabaseWrapper<A, DB>
where
    A: AsyncExecutor,
    DB: LocalAsyncDatabase + LocalAsyncOptionable<Option = OptionDatabase>,
    DB::ConnectionType: LocalAsyncConnection + LocalAsyncOptionable<Option = OptionConnection>,
    <DB::ConnectionType as LocalAsyncConnection>::StatementType:
        LocalAsyncOptionable<Option = OptionStatement>,
{
    type ConnectionType = SyncConnectionWrapper<A, DB::ConnectionType>;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let conn = self.executor.block_on(self.inner.new_connection())?;

        Ok(SyncConnectionWrapper {
            inner: conn,
            executor: self.executor.clone(),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let conn = self.executor.block_on(
            self.inner
                .new_connection_with_opts(opts.into_iter().collect()),
        )?;

        Ok(SyncConnectionWrapper {
            inner: conn,
            executor: self.executor.clone(),
        })
    }
}

pub struct SyncConnectionWrapper<A, Conn> {
    inner: Conn,
    executor: Arc<A>,
}

impl<A, Conn> Optionable for SyncConnectionWrapper<A, Conn>
where
    A: AsyncExecutor,
    Conn: LocalAsyncOptionable<Option = OptionConnection>,
{
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.executor.block_on(self.inner.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.executor.block_on(self.inner.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.executor.block_on(self.inner.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.executor.block_on(self.inner.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.executor.block_on(self.inner.get_option_double(key))
    }
}

impl<A, Conn> Connection for SyncConnectionWrapper<A, Conn>
where
    A: AsyncExecutor,
    Conn: LocalAsyncConnection + LocalAsyncOptionable<Option = OptionConnection>,
    Conn::StatementType: LocalAsyncOptionable<Option = OptionStatement>,
{
    type StatementType = SyncStatementWrapper<A, Conn::StatementType>;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let stmt = self.executor.block_on(self.inner.new_statement())?;

        Ok(SyncStatementWrapper {
            inner: stmt,
            executor: self.executor.clone(),
        })
    }

    fn cancel(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.cancel())
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        let reader = self.executor.block_on(self.inner.get_info(codes))?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        let reader = self.executor.block_on(self.inner.get_objects(
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.executor
            .block_on(self.inner.get_table_schema(catalog, db_schema, table_name))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        let reader = self.executor.block_on(self.inner.get_table_types())?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        let reader = self.executor.block_on(self.inner.get_statistic_names())?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        let reader = self.executor.block_on(self.inner.get_statistics(
            catalog,
            db_schema,
            table_name,
            approximate,
        ))?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }

    fn commit(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.commit())
    }

    fn rollback(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.rollback())
    }

    fn read_partition(&self, partition: &[u8]) -> Result<impl RecordBatchReader + Send> {
        let reader = self
            .executor
            .block_on(self.inner.read_partition(partition))?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }
}

pub struct SyncStatementWrapper<A, T> {
    inner: T,
    executor: Arc<A>,
}

impl<A, Stmt> Optionable for SyncStatementWrapper<A, Stmt>
where
    A: AsyncExecutor,
    Stmt: LocalAsyncOptionable<Option = OptionStatement>,
{
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.executor.block_on(self.inner.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.executor.block_on(self.inner.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.executor.block_on(self.inner.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.executor.block_on(self.inner.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.executor.block_on(self.inner.get_option_double(key))
    }
}

impl<A, Stmt> Statement for SyncStatementWrapper<A, Stmt>
where
    A: AsyncExecutor,
    Stmt: LocalAsyncStatement + LocalAsyncOptionable<Option = OptionStatement>,
{
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.executor.block_on(self.inner.bind(batch))
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.executor.block_on(
            self.inner
                .bind_stream(Box::pin(WrapperRecordBatchReader { inner: reader })),
        )
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let reader = self.executor.block_on(self.inner.execute())?;

        Ok(SyncRecordBatchStream {
            executor: self.executor.clone(),
            inner: reader,
        })
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.executor.block_on(self.inner.execute_update())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        self.executor.block_on(self.inner.execute_schema())
    }

    fn execute_partitions(&mut self) -> Result<crate::PartitionedResult> {
        self.executor.block_on(self.inner.execute_partitions())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        self.executor.block_on(self.inner.get_parameter_schema())
    }

    fn prepare(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.prepare())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.executor
            .block_on(self.inner.set_sql_query(query.as_ref()))
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        self.executor
            .block_on(self.inner.set_substrait_plan(plan.as_ref()))
    }

    fn cancel(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.cancel())
    }
}
