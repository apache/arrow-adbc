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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Mutex;

use adbc_core::{
  options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
  },
  Connection, Database, Driver, Optionable, Statement, LOAD_FLAG_DEFAULT,
};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver, ManagedStatement};
use arrow_array::RecordBatchReader;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
  #[error("ADBC Error: {0}")]
  Adbc(#[from] adbc_core::error::Error),
  #[error("Arrow Error: {0}")]
  Arrow(#[from] arrow_schema::ArrowError),
  #[error("Other Error: {0}")]
  Other(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

pub struct ConnectOptions {
  pub driver: String,
  pub entrypoint: Option<String>,
  pub manifest_search_paths: Option<Vec<String>>,
  pub profile_search_paths: Option<Vec<String>>,
  pub load_flags: Option<u32>,
  pub database_options: Option<HashMap<String, String>>,
}

pub struct GetObjectsOptions {
  pub depth: i32,
  pub catalog: Option<String>,
  pub db_schema: Option<String>,
  pub table_name: Option<String>,
  pub table_type: Option<Vec<String>>,
  pub column_name: Option<String>,
}

pub struct GetTableSchemaOptions {
  pub catalog: Option<String>,
  pub db_schema: Option<String>,
  pub table_name: String,
}

pub struct AdbcDatabaseCore {
  inner: ManagedDatabase,
}

impl AdbcDatabaseCore {
  pub fn new(opts: ConnectOptions) -> Result<Self> {
    let version = AdbcVersion::V110;
    let load_flags = opts.load_flags.unwrap_or(LOAD_FLAG_DEFAULT);
    let entrypoint = opts.entrypoint.as_ref().map(|s| s.as_bytes().to_vec());

    let manifest_search_paths: Option<Vec<PathBuf>> = opts
      .manifest_search_paths
      .map(|paths| paths.into_iter().map(PathBuf::from).collect());

    let profile_search_paths: Option<Vec<PathBuf>> = opts
      .profile_search_paths
      .map(|paths| paths.into_iter().map(PathBuf::from).collect());

    let database_opts = opts.database_options.map(map_database_options);

    let database = if opts.driver.contains(':') {
      let provider = adbc_driver_manager::profile::FilesystemProfileProvider::new_with_search_paths(
        profile_search_paths,
      );
      // URI-style ("sqlite:file::memory:") or profile URI ("profile://my_profile")
      ManagedDatabase::from_uri_with_profile_provider(
        &opts.driver,
        entrypoint.as_deref(),
        version,
        load_flags,
        manifest_search_paths,
        provider,
        database_opts.into_iter().flatten(),
      )?
    } else {
      // Short name ("sqlite") or path ("/usr/lib/libadbc_driver_sqlite.so")
      let mut driver = ManagedDriver::load_from_name(
        &opts.driver,
        entrypoint.as_deref(),
        version,
        load_flags,
        manifest_search_paths,
      )?;
      match database_opts {
        Some(db_opts) => driver.new_database_with_opts(db_opts)?,
        None => driver.new_database()?,
      }
    };

    Ok(Self { inner: database })
  }

  pub fn connect(&self, options: Option<HashMap<String, String>>) -> Result<AdbcConnectionCore> {
    let conn = if let Some(opts) = options {
      self
        .inner
        .new_connection_with_opts(map_connection_options(opts))?
    } else {
      self.inner.new_connection()?
    };
    Ok(AdbcConnectionCore {
      inner: Mutex::new(conn),
    })
  }
}

pub struct AdbcConnectionCore {
  inner: Mutex<ManagedConnection>,
}

impl AdbcConnectionCore {
  pub fn new_statement(&self) -> Result<AdbcStatementCore> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let stmt = conn.new_statement()?;
    Ok(AdbcStatementCore { inner: stmt })
  }

  pub fn set_option(&self, key: &str, value: &str) -> Result<()> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    conn.set_option(
      OptionConnection::Other(key.to_string()),
      OptionValue::String(value.to_string()),
    )?;
    Ok(())
  }

  pub fn get_objects(&self, opts: GetObjectsOptions) -> Result<AdbcResultIteratorCore> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let depth = match opts.depth {
      1 => ObjectDepth::Catalogs,
      2 => ObjectDepth::Schemas,
      3 => ObjectDepth::Tables,
      _ => ObjectDepth::All,
    };

    let table_types_str: Option<Vec<&str>> = opts
      .table_type
      .as_ref()
      .map(|v| v.iter().map(|s| s.as_str()).collect());

    let reader = conn.get_objects(
      depth,
      opts.catalog.as_deref(),
      opts.db_schema.as_deref(),
      opts.table_name.as_deref(),
      table_types_str,
      opts.column_name.as_deref(),
    )?;

    Ok(AdbcResultIteratorCore {
      reader,
      exhausted: false,
    })
  }

  pub fn get_table_schema(&self, opts: GetTableSchemaOptions) -> Result<Vec<u8>> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let schema = conn.get_table_schema(
      opts.catalog.as_deref(),
      opts.db_schema.as_deref(),
      &opts.table_name,
    )?;

    let mut output = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, &schema)?;
    writer.finish()?;
    Ok(output)
  }

  pub fn get_table_types(&self) -> Result<AdbcResultIteratorCore> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let reader = conn.get_table_types()?;

    Ok(AdbcResultIteratorCore {
      reader,
      exhausted: false,
    })
  }

  pub fn get_info(&self, info_codes: Option<Vec<u32>>) -> Result<AdbcResultIteratorCore> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let codes: Option<HashSet<InfoCode>> = info_codes.map(|v| {
      v.into_iter()
        .filter_map(|code| InfoCode::try_from(code).ok())
        .collect::<HashSet<InfoCode>>()
    });
    let reader = conn.get_info(codes)?;

    Ok(AdbcResultIteratorCore {
      reader,
      exhausted: false,
    })
  }

  pub fn commit(&self) -> Result<()> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    conn.commit()?;
    Ok(())
  }

  pub fn rollback(&self) -> Result<()> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    conn.rollback()?;
    Ok(())
  }
}

pub struct AdbcStatementCore {
  inner: ManagedStatement,
}

impl AdbcStatementCore {
  pub fn set_sql_query(&mut self, query: &str) -> Result<()> {
    self.inner.set_sql_query(query)?;
    Ok(())
  }

  pub fn set_option(&mut self, key: &str, value: &str) -> Result<()> {
    self.inner.set_option(
      OptionStatement::Other(key.to_string()),
      OptionValue::String(value.to_string()),
    )?;
    Ok(())
  }

  pub fn execute_query(&mut self) -> Result<AdbcResultIteratorCore> {
    let reader = self.inner.execute()?;

    Ok(AdbcResultIteratorCore {
      reader,
      exhausted: false,
    })
  }

  pub fn execute_update(&mut self) -> Result<i64> {
    let rows = self.inner.execute_update()?;
    Ok(rows.unwrap_or(-1))
  }

  pub fn bind(&mut self, c_data: Vec<u8>) -> Result<()> {
    let reader =
      StreamReader::try_new(std::io::Cursor::new(c_data), None).map_err(ClientError::Arrow)?;
    self.inner.bind_stream(Box::new(reader))?;
    Ok(())
  }

  pub fn bind_channel_stream(
    &mut self,
    schema_bytes: Vec<u8>,
    receiver: mpsc::Receiver<Vec<u8>>,
  ) -> Result<()> {
    let reader = ChannelBatchReader::new(schema_bytes, receiver)?;
    self.inner.bind_stream(Box::new(reader))?;
    Ok(())
  }

  pub fn bind_stream_and_execute(
    &mut self,
    schema_bytes: Vec<u8>,
    receiver: mpsc::Receiver<Vec<u8>>,
  ) -> Result<i64> {
    self.bind_channel_stream(schema_bytes, receiver)?;
    let rows = self.inner.execute_update()?;
    Ok(rows.unwrap_or(-1))
  }
}

pub struct AdbcResultIteratorCore {
  reader: Box<dyn RecordBatchReader + Send>,
  exhausted: bool,
}

impl AdbcResultIteratorCore {
  pub fn next(&mut self) -> Result<Option<Vec<u8>>> {
    if self.exhausted {
      return Ok(None);
    }
    self.exhausted = true;

    let schema = self.reader.schema();
    let mut output = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, &schema)?;
    for batch in self.reader.by_ref() {
      writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(Some(output))
  }
}

/// A `RecordBatchReader` backed by a channel. Batches arrive as IPC bytes
/// from the JS main thread and are deserialized on demand by the thread pool.
pub struct ChannelBatchReader {
  schema: SchemaRef,
  receiver: mpsc::Receiver<Vec<u8>>,
}

impl ChannelBatchReader {
  pub fn new(schema_bytes: Vec<u8>, receiver: mpsc::Receiver<Vec<u8>>) -> Result<Self> {
    let ipc_reader = StreamReader::try_new(std::io::Cursor::new(schema_bytes), None)
      .map_err(ClientError::Arrow)?;
    let schema = ipc_reader.schema();
    Ok(Self { schema, receiver })
  }
}

impl Iterator for ChannelBatchReader {
  type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

  fn next(&mut self) -> Option<Self::Item> {
    let bytes = self.receiver.recv().ok()?;
    let mut ipc_reader = match StreamReader::try_new(std::io::Cursor::new(bytes), None) {
      Ok(r) => r,
      Err(e) => return Some(Err(e)),
    };
    match ipc_reader.next() {
      Some(result) => Some(result),
      None => Some(Err(arrow_schema::ArrowError::IpcError(
        "Received IPC stream with no record batches".to_string(),
      ))),
    }
  }
}

impl RecordBatchReader for ChannelBatchReader {
  fn schema(&self) -> SchemaRef {
    self.schema.clone()
  }
}

fn map_database_options(
  opts: HashMap<String, String>,
) -> impl Iterator<Item = (OptionDatabase, OptionValue)> {
  opts.into_iter().map(|(k, v)| {
    let key = match k.as_str() {
      "uri" => OptionDatabase::Uri,
      "user" => OptionDatabase::Username,
      "password" => OptionDatabase::Password,
      other => OptionDatabase::Other(other.to_string()),
    };
    (key, OptionValue::String(v))
  })
}

fn map_connection_options(
  opts: HashMap<String, String>,
) -> impl Iterator<Item = (OptionConnection, OptionValue)> {
  opts
    .into_iter()
    .map(|(k, v)| (OptionConnection::Other(k), OptionValue::String(v)))
}
