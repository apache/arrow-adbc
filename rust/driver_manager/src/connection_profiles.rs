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

use crate::search::find_filesystem_profile;
use crate::ManagedDatabase;
use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
    Optionable,
};
use adbc_ffi::FFI_AdbcDriverInitFunc;
use std::fmt;
use std::path::PathBuf;
use std::{collections::HashMap, fs};
use toml::de::{DeTable, DeValue};

pub trait ConnectionProfile {
    fn get_driver_name(&self) -> Result<(&String, Option<&FFI_AdbcDriverInitFunc>)>;
    fn get_options(
        &self,
    ) -> Result<impl IntoIterator<Item = (<ManagedDatabase as Optionable>::Option, OptionValue)>>;
}

pub trait ConnectionProfileProvider {
    type Profile: ConnectionProfile;
    fn get_profile(
        &self,
        name: &str,
        additional_path_list: Option<Vec<PathBuf>>,
    ) -> Result<Self::Profile>;
}

pub struct FilesystemProfileProvider {}

impl ConnectionProfileProvider for FilesystemProfileProvider {
    type Profile = FilesystemProfile;

    fn get_profile(
        &self,
        name: &str,
        additional_path_list: Option<Vec<PathBuf>>,
    ) -> Result<Self::Profile> {
        let profile_path = find_filesystem_profile(name, additional_path_list)?;
        FilesystemProfile::from_path(profile_path)
    }
}

fn process_options(
    opts: &mut HashMap<OptionDatabase, OptionValue>,
    prefix: String,
    table: &DeTable,
) -> Result<()> {
    for (key, value) in table.iter() {
        let full_key = prefix.clone() + key.get_ref();
        match value.get_ref() {
            DeValue::String(s) => {
                opts.insert(full_key.as_str().into(), OptionValue::String(s.to_string()));
            }
            DeValue::Integer(i) => {
                let val: i64 = i.as_str().parse().map_err(|e| {
                    Error::with_message_and_status(
                        format!("invalid integer value for key '{}': {e}", full_key),
                        Status::InvalidArguments,
                    )
                })?;
                opts.insert(full_key.as_str().into(), OptionValue::Int(val));
            }
            DeValue::Float(f) => {
                let val: f64 = f.as_str().parse().map_err(|e| {
                    Error::with_message_and_status(
                        format!("invalid float value for key '{}': {e}", full_key),
                        Status::InvalidArguments,
                    )
                })?;
                opts.insert(full_key.as_str().into(), OptionValue::Double(val));
            }
            DeValue::Boolean(b) => {
                opts.insert(full_key.as_str().into(), OptionValue::String(b.to_string()));
            }
            DeValue::Table(t) => {
                process_options(opts, full_key + ".", t)?;
            }
            _ => {
                return Err(Error::with_message_and_status(
                    format!("unsupported option type for key '{}'", full_key),
                    Status::InvalidArguments,
                ));
            }
        }
    }
    Ok(())
}

pub struct FilesystemProfile {
    profile_path: PathBuf,
    driver: String,
    opts: HashMap<OptionDatabase, OptionValue>,
}

impl FilesystemProfile {
    fn from_path(profile_path: PathBuf) -> Result<Self> {
        let contents = fs::read_to_string(&profile_path).map_err(|e| {
            Error::with_message_and_status(
                format!("could not read profile '{}': {e}", profile_path.display()),
                Status::InvalidArguments,
            )
        })?;

        let profile = DeTable::parse(&contents)
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))?;

        let profile_version = profile
            .get_ref()
            .get("version")
            .and_then(|v| v.get_ref().as_integer())
            .map(|v| v.as_str())
            .unwrap_or("1");

        if profile_version != "1" {
            return Err(Error::with_message_and_status(
                format!(
                    "unsupported profile version '{}', expected '1'",
                    profile_version
                ),
                Status::InvalidArguments,
            ));
        }

        let driver = profile
            .get_ref()
            .get("driver")
            .and_then(|v| v.get_ref().as_str())
            .unwrap_or("")
            .to_string();

        let options_table = profile
            .get_ref()
            .get("options")
            .and_then(|v| v.get_ref().as_table())
            .ok_or_else(|| {
                Error::with_message_and_status(
                    "missing or invalid 'options' table in profile".to_string(),
                    Status::InvalidArguments,
                )
            })?;

        let mut opts = HashMap::new();
        process_options(&mut opts, "".to_string(), options_table)?;

        Ok(FilesystemProfile {
            profile_path,
            driver,
            opts,
        })
    }
}

impl ConnectionProfile for FilesystemProfile {
    fn get_driver_name(&self) -> Result<(&String, Option<&FFI_AdbcDriverInitFunc>)> {
        Ok((&self.driver, None))
    }

    fn get_options(
        &self,
    ) -> Result<impl IntoIterator<Item = (<ManagedDatabase as Optionable>::Option, OptionValue)>>
    {
        Ok(self.opts.clone().into_iter())
    }
}

impl fmt::Display for FilesystemProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilesystemProfile({})", self.profile_path.display())
    }
}
