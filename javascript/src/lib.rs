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

mod client;
use crate::client::{
  AdbcConnectionCore as CoreConnection, AdbcDatabaseCore as CoreDatabase,
  AdbcResultIteratorCore as CoreResultIterator, AdbcStatementCore as CoreStatement, ClientError,
  ConnectOptions as CoreConnectOptions, GetObjectsOptions as CoreGetObjectsOptions,
  GetTableSchemaOptions as CoreGetTableSchemaOptions,
};
use adbc_core::options::AdbcVersion;
use napi::bindgen_prelude::{
  AsyncTask, Buffer, Error, JsObjectValue, Result, Status, ToNapiValue, Unknown,
};
use napi::{Env, Task};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[macro_use]
extern crate napi_derive;

fn to_napi_err(err: ClientError) -> Error {
  match err {
    ClientError::Adbc(e) => Error::new(Status::GenericFailure, e.message),
    ClientError::Arrow(e) => Error::new(Status::GenericFailure, format!("Arrow Error: {e}")),
    ClientError::Other(e) => Error::new(Status::GenericFailure, format!("Internal Error: {e}")),
  }
}

/// Converts an ADBC error into a `napi::Error` whose JS form is a structured `AdbcError`
/// object with `name`, `code`, `vendorCode`, and `sqlState` properties.
///
/// The JS object is stored via `napi_ref` (napi-rs's `maybe_raw` field) so napi-rs uses it
/// directly as the throw/rejection value. This avoids `env.throw()`, which sets a V8 pending
/// exception that would escape `Task::reject` as an uncaughtException instead of rejecting
/// the Promise.
fn build_adbc_err(env: Env, ae: adbc_core::error::Error) -> Error {
  let adbc_core::error::Error {
    message,
    status,
    vendor_code,
    sqlstate,
    ..
  } = ae;
  // details (ADBC 1.1.0 key-value metadata) are not yet exposed to JS.
  let Ok(mut js_err) = env.create_error(Error::new(Status::GenericFailure, message.as_str()))
  else {
    return Error::new(Status::GenericFailure, message);
  };
  let _ = js_err.set_named_property("name", "AdbcError");
  let _ = js_err.set_named_property("code", format!("{status:?}").as_str());
  if vendor_code != i32::MIN {
    let _ = js_err.set_named_property("vendorCode", vendor_code);
  }
  let sqlstate_bytes: [u8; 5] = sqlstate.map(|c| c as u8);
  if sqlstate_bytes.iter().any(|&b| b != 0) {
    let sql_state = std::str::from_utf8(&sqlstate_bytes).unwrap_or("");
    let _ = js_err.set_named_property("sqlState", sql_state);
  }
  let raw_env = env.raw();
  match unsafe { ToNapiValue::to_napi_value(raw_env, js_err) } {
    Ok(raw_val) => Error::from(unsafe { Unknown::from_raw_unchecked(raw_env, raw_val) }),
    Err(fallback) => fallback,
  }
}

/// For synchronous `#[napi]` methods: converts a `ClientError` to a `napi::Error` that,
/// when propagated by napi-rs, throws a structured `AdbcError` JS object.
fn sync_adbc_err(err: ClientError, env: Env) -> Error {
  match err {
    ClientError::Adbc(ae) => build_adbc_err(env, ae),
    other => to_napi_err(other),
  }
}

/// Captures an ADBC error from a thread-pool `compute()` result into `sink` so that
/// `reject()` (main JS thread) can later call `build_adbc_err` with an `Env`.
fn capture_adbc_err<T>(
  result: crate::client::Result<T>,
  sink: &mut Option<adbc_core::error::Error>,
) -> Result<T> {
  result.map_err(|e| match e {
    ClientError::Adbc(ae) => {
      let napi_err = Error::new(Status::GenericFailure, ae.message.as_str());
      *sink = Some(ae);
      napi_err
    }
    other => to_napi_err(other),
  })
}

fn reject_adbc<T>(
  env: Env,
  adbc_err: &mut Option<adbc_core::error::Error>,
  err: Error,
) -> Result<T> {
  Err(
    adbc_err
      .take()
      .map(|ae| build_adbc_err(env, ae))
      .unwrap_or(err),
  )
}

fn closed_err() -> Error {
  Error::new(Status::GenericFailure, "Object is closed")
}

#[napi]
pub fn crate_version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}

#[napi]
pub fn default_adbc_version() -> String {
  match AdbcVersion::default() {
    AdbcVersion::V100 => "1.0.0".to_string(),
    AdbcVersion::V110 => "1.1.0".to_string(),
    _ => "unknown".to_string(),
  }
}

#[napi]
pub fn default_load_flags() -> u32 {
  adbc_core::LOAD_FLAG_DEFAULT
}

// Options
#[napi(object)]
pub struct ConnectOptions {
  pub driver: String,
  pub entrypoint: Option<String>,
  pub search_paths: Option<Vec<String>>,
  pub load_flags: Option<u32>,
  pub database_options: Option<HashMap<String, String>>,
}

impl From<ConnectOptions> for CoreConnectOptions {
  fn from(opts: ConnectOptions) -> Self {
    Self {
      driver: opts.driver,
      entrypoint: opts.entrypoint,
      search_paths: opts.search_paths,
      load_flags: opts.load_flags,
      database_options: opts.database_options,
    }
  }
}

#[napi(object)]
pub struct GetObjectsOptions {
  pub depth: i32,
  pub catalog: Option<String>,
  pub db_schema: Option<String>,
  pub table_name: Option<String>,
  pub table_type: Option<Vec<String>>,
  pub column_name: Option<String>,
}

impl From<GetObjectsOptions> for CoreGetObjectsOptions {
  fn from(opts: GetObjectsOptions) -> Self {
    Self {
      depth: opts.depth,
      catalog: opts.catalog,
      db_schema: opts.db_schema,
      table_name: opts.table_name,
      table_type: opts.table_type,
      column_name: opts.column_name,
    }
  }
}

#[napi(object)]
pub struct GetTableSchemaOptions {
  pub catalog: Option<String>,
  pub db_schema: Option<String>,
  pub table_name: String,
}

impl From<GetTableSchemaOptions> for CoreGetTableSchemaOptions {
  fn from(opts: GetTableSchemaOptions) -> Self {
    Self {
      catalog: opts.catalog,
      db_schema: opts.db_schema,
      table_name: opts.table_name,
    }
  }
}

// --- Database ---

#[napi]
pub struct _NativeAdbcDatabase {
  inner: Option<Arc<CoreDatabase>>,
}

#[napi]
impl _NativeAdbcDatabase {
  #[napi(constructor)]
  pub fn new(opts: ConnectOptions) -> Result<Self> {
    let db = CoreDatabase::new(opts.into()).map_err(to_napi_err)?;
    Ok(Self {
      inner: Some(Arc::new(db)),
    })
  }

  #[napi]
  pub fn connect(
    &self,
    options: Option<HashMap<String, String>>,
  ) -> Result<AsyncTask<ConnectTask>> {
    let db = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(ConnectTask {
      database: db.clone(),
      options,
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn close(&mut self) -> Result<()> {
    self.inner.take();
    Ok(())
  }
}

pub struct ConnectTask {
  database: Arc<CoreDatabase>,
  options: Option<HashMap<String, String>>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for ConnectTask {
  type Output = CoreConnection;
  type JsValue = _NativeAdbcConnection;

  fn compute(&mut self) -> Result<Self::Output> {
    capture_adbc_err(
      self.database.connect(self.options.take()),
      &mut self.adbc_err,
    )
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(_NativeAdbcConnection {
      inner: Some(Arc::new(output)),
    })
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

// --- Connection ---

#[napi]
pub struct _NativeAdbcConnection {
  inner: Option<Arc<CoreConnection>>,
}

#[napi]
impl _NativeAdbcConnection {
  #[napi]
  pub fn create_statement(&self) -> Result<AsyncTask<CreateStatementTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(CreateStatementTask {
      connection: conn.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn set_option(&self, env: Env, key: String, value: String) -> Result<()> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    conn
      .set_option(&key, &value)
      .map_err(|e| sync_adbc_err(e, env))
  }

  #[napi]
  pub fn get_objects(&self, opts: GetObjectsOptions) -> Result<AsyncTask<GetObjectsTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(GetObjectsTask {
      connection: conn.clone(),
      options: Some(opts.into()),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn get_table_schema(
    &self,
    opts: GetTableSchemaOptions,
  ) -> Result<AsyncTask<GetTableSchemaTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(GetTableSchemaTask {
      connection: conn.clone(),
      options: Some(opts.into()),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn get_table_types(&self) -> Result<AsyncTask<GetTableTypesTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(GetTableTypesTask {
      connection: conn.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn get_info(&self, info_codes: Option<Vec<u32>>) -> Result<AsyncTask<GetInfoTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(GetInfoTask {
      connection: conn.clone(),
      info_codes,
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn commit(&self) -> Result<AsyncTask<CommitTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(CommitTask {
      connection: conn.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn rollback(&self) -> Result<AsyncTask<RollbackTask>> {
    let conn = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(RollbackTask {
      connection: conn.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn close(&mut self) -> Result<()> {
    self.inner.take();
    Ok(())
  }
}

pub struct CreateStatementTask {
  connection: Arc<CoreConnection>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for CreateStatementTask {
  type Output = CoreStatement;
  type JsValue = _NativeAdbcStatement;

  fn compute(&mut self) -> Result<Self::Output> {
    capture_adbc_err(self.connection.new_statement(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(_NativeAdbcStatement {
      inner: Some(Arc::new(Mutex::new(output))),
    })
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

// Metadata Tasks

pub struct GetObjectsTask {
  connection: Arc<CoreConnection>,
  options: Option<CoreGetObjectsOptions>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for GetObjectsTask {
  type Output = CoreResultIterator;
  type JsValue = _NativeAdbcResultIterator;

  fn compute(&mut self) -> Result<Self::Output> {
    let opts = self.options.take().expect("compute called twice");
    capture_adbc_err(self.connection.get_objects(opts), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(_NativeAdbcResultIterator {
      inner: Some(Arc::new(Mutex::new(output))),
    })
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct GetTableSchemaTask {
  connection: Arc<CoreConnection>,
  options: Option<CoreGetTableSchemaOptions>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for GetTableSchemaTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let opts = self.options.take().expect("compute called twice");
    capture_adbc_err(self.connection.get_table_schema(opts), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct GetTableTypesTask {
  connection: Arc<CoreConnection>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for GetTableTypesTask {
  type Output = CoreResultIterator;
  type JsValue = _NativeAdbcResultIterator;

  fn compute(&mut self) -> Result<Self::Output> {
    capture_adbc_err(self.connection.get_table_types(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(_NativeAdbcResultIterator {
      inner: Some(Arc::new(Mutex::new(output))),
    })
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct GetInfoTask {
  connection: Arc<CoreConnection>,
  info_codes: Option<Vec<u32>>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for GetInfoTask {
  type Output = CoreResultIterator;
  type JsValue = _NativeAdbcResultIterator;

  fn compute(&mut self) -> Result<Self::Output> {
    capture_adbc_err(
      self.connection.get_info(self.info_codes.take()),
      &mut self.adbc_err,
    )
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(_NativeAdbcResultIterator {
      inner: Some(Arc::new(Mutex::new(output))),
    })
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct CommitTask {
  connection: Arc<CoreConnection>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for CommitTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> Result<Self::Output> {
    capture_adbc_err(self.connection.commit(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    Ok(())
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct RollbackTask {
  connection: Arc<CoreConnection>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for RollbackTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> Result<Self::Output> {
    capture_adbc_err(self.connection.rollback(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    Ok(())
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

// --- Statement ---

#[napi]
pub struct _NativeAdbcStatement {
  inner: Option<Arc<Mutex<CoreStatement>>>,
}

#[napi]
impl _NativeAdbcStatement {
  #[napi]
  pub fn set_sql_query(&self, env: Env, query: String) -> Result<()> {
    let mutex = self.inner.as_ref().ok_or_else(closed_err)?;
    let mut stmt = mutex
      .lock()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    stmt
      .set_sql_query(&query)
      .map_err(|e| sync_adbc_err(e, env))
  }

  #[napi]
  pub fn set_option(&self, env: Env, key: String, value: String) -> Result<()> {
    let mutex = self.inner.as_ref().ok_or_else(closed_err)?;
    let mut stmt = mutex
      .lock()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    stmt
      .set_option(&key, &value)
      .map_err(|e| sync_adbc_err(e, env))
  }

  #[napi]
  pub fn execute_query(&self) -> Result<AsyncTask<ExecuteQueryTask>> {
    let mutex = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(ExecuteQueryTask {
      statement: mutex.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn execute_update(&self) -> Result<AsyncTask<ExecuteUpdateTask>> {
    let mutex = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(ExecuteUpdateTask {
      statement: mutex.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn bind(&self, data: Buffer) -> Result<AsyncTask<BindTask>> {
    let mutex = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(BindTask {
      statement: mutex.clone(),
      data: data.to_vec(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn close(&mut self) -> Result<()> {
    self.inner.take();
    Ok(())
  }
}

pub struct ExecuteQueryTask {
  statement: Arc<Mutex<CoreStatement>>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for ExecuteQueryTask {
  type Output = CoreResultIterator;
  type JsValue = _NativeAdbcResultIterator;

  fn compute(&mut self) -> Result<Self::Output> {
    let mut stmt = self
      .statement
      .lock()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    capture_adbc_err(stmt.execute_query(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(_NativeAdbcResultIterator {
      inner: Some(Arc::new(Mutex::new(output))),
    })
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct ExecuteUpdateTask {
  statement: Arc<Mutex<CoreStatement>>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for ExecuteUpdateTask {
  type Output = i64;
  type JsValue = i64;

  fn compute(&mut self) -> Result<Self::Output> {
    let mut stmt = self
      .statement
      .lock()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    capture_adbc_err(stmt.execute_update(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

pub struct BindTask {
  statement: Arc<Mutex<CoreStatement>>,
  data: Vec<u8>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for BindTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> Result<Self::Output> {
    let mut stmt = self
      .statement
      .lock()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    capture_adbc_err(
      stmt.bind(std::mem::take(&mut self.data)),
      &mut self.adbc_err,
    )
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    Ok(())
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

// --- Iterators ---

pub struct IteratorNextTask {
  iterator: Arc<Mutex<CoreResultIterator>>,
  adbc_err: Option<adbc_core::error::Error>,
}

impl Task for IteratorNextTask {
  type Output = Option<Vec<u8>>;
  type JsValue = Option<Buffer>;

  fn compute(&mut self) -> Result<Self::Output> {
    let mut iterator = self
      .iterator
      .lock()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    capture_adbc_err(iterator.next(), &mut self.adbc_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output.map(Buffer::from))
  }

  fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
    reject_adbc(env, &mut self.adbc_err, err)
  }
}

#[napi]
pub struct _NativeAdbcResultIterator {
  inner: Option<Arc<Mutex<CoreResultIterator>>>,
}

#[napi]
impl _NativeAdbcResultIterator {
  #[napi]
  pub fn next(&self) -> Result<AsyncTask<IteratorNextTask>> {
    let iterator = self.inner.as_ref().ok_or_else(closed_err)?;
    Ok(AsyncTask::new(IteratorNextTask {
      iterator: iterator.clone(),
      adbc_err: None,
    }))
  }

  #[napi]
  pub fn close(&mut self) -> Result<()> {
    self.inner.take();
    Ok(())
  }
}
