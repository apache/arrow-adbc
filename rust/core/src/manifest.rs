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

//! Driver manifest.

use std::{
    collections::HashMap,
    fmt::Display,
    marker::PhantomData,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use toml::Value;
use void::Void;

use crate::{
    error::{Error, Status},
    options::AdbcVersion,
};

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Manifest {
    /// Driver display name
    name: Option<String>,

    /// Publisher Identifier
    publisher: Option<String>,

    /// Driver license
    #[serde(default, deserialize_with = "de_from_str_opt")]
    license: Option<spdx::Expression>,

    /// Driver Version
    version: Option<semver::Version>,

    /// Source of the driver, e.g. a URL to the source code repository
    source: Option<String>,

    /// ADBC
    #[serde(rename = "ADBC")]
    adbc: Option<ADBC>,

    /// Driver
    #[serde(rename = "Driver")]
    driver: Driver,

    /// Additional Fields
    #[serde(flatten)]
    additional: HashMap<String, Value>,
}

impl Manifest {
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn publisher(&self) -> Option<&str> {
        self.publisher.as_deref()
    }

    pub fn license(&self) -> Option<&spdx::Expression> {
        self.license.as_ref()
    }

    pub fn version(&self) -> Option<&semver::Version> {
        self.version.as_ref()
    }

    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn adbc(&self) -> Option<&ADBC> {
        self.adbc.as_ref()
    }

    pub fn driver(&self) -> &Driver {
        &self.driver
    }

    pub fn additional(&self) -> &HashMap<String, Value> {
        &self.additional
    }
}

/// Deserialize an `Option<T>` using the [`FromStr`] impl of `T`
fn de_from_str_opt<'de, D, T: FromStr<Err: Display>>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).and_then(|value| {
        value
            .as_deref()
            .map(|value| T::from_str(value).map_err(de::Error::custom))
            .transpose()
    })
}

impl FromStr for Manifest {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        toml::from_str(input)
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))
    }
}

fn string_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = Void>,
    D: Deserializer<'de>,
{
    // visitor that forwards string types to T's `FromStr` impl
    // and forwards map types to T's `Deserialize` impl. The `PhantomData`
    // is to keep the compiler from compaining about T being an unused
    // generic type parameter. We need T in order to know the Value type
    // for the Visitor impl.
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr<Err = Void>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where
            E: de::Error,
        {
            Ok(FromStr::from_str(value).unwrap())
        }

        fn visit_map<M>(self, map: M) -> Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` impl. T then deserializes itself using the entries
            // from the map visitor.
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

/// ADBC Information
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ADBC {
    /// Supported ADBC spec version
    #[serde(deserialize_with = "de_from_str_opt")]
    version: Option<AdbcVersion>,

    /// Features
    features: Option<Features>,
}

impl ADBC {
    pub fn version(&self) -> Option<AdbcVersion> {
        self.version
    }

    pub fn features(&self) -> Option<&Features> {
        self.features.as_ref()
    }
}

/// ADBC Features
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Features {
    /// List of supported features
    supported: Option<Vec<String>>,

    unsupported: Option<Vec<String>>,
}

impl Features {
    pub fn supported(&self) -> Option<&[String]> {
        self.supported.as_deref()
    }

    pub fn unsupported(&self) -> Option<&[String]> {
        self.unsupported.as_deref()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Driver {
    #[serde(deserialize_with = "string_or_struct")]
    shared: DriverPath,

    entrypoint: Option<String>,
}

impl Driver {
    pub fn shared(&self) -> &Path {
        self.shared.path()
    }

    pub fn entrypoint(&self) -> Option<&str> {
        self.entrypoint.as_deref()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct DriverPath {
    #[serde(default)]
    default_path: PathBuf,

    #[serde(flatten)]
    arch_paths: HashMap<String, PathBuf>,
}

impl DriverPath {
    pub fn path(&self) -> &Path {
        let (arch_os, arch_arch, arch_extra) = arch_triplet();
        let arch_key = format!("{arch_os}_{arch_arch}{arch_extra}");
        if let Some(path) = self.arch_paths.get(&arch_key) {
            path.as_path()
        } else {
            self.default_path.as_path()
        }
    }
}

impl FromStr for DriverPath {
    type Err = Void;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(DriverPath {
            default_path: PathBuf::from(s),
            arch_paths: HashMap::new(),
        })
    }
}

pub const fn arch_triplet() -> (&'static str, &'static str, &'static str) {
    #[cfg(target_arch = "x86_64")]
    const ARCH: &str = "amd64";
    #[cfg(target_arch = "aarch64")]
    const ARCH: &str = "arm64";
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    const ARCH: &str = std::env::consts::ARCH;

    #[cfg(target_os = "macos")]
    const OS: &str = "osx";
    #[cfg(not(target_os = "macos"))]
    const OS: &str = std::env::consts::OS;

    #[cfg(target_env = "musl")]
    const EXTRA: &str = "_musl";
    #[cfg(all(target_os = "windows", target_env = "gnu"))]
    const EXTRA: &str = "_mingw";
    #[cfg(not(any(target_env = "musl", all(target_os = "windows", target_env = "gnu"))))]
    const EXTRA: &str = "";

    (OS, ARCH, EXTRA)
}

#[cfg(test)]
mod tests {
    use crate::error;

    use super::*;

    #[test]
    fn minimal() -> error::Result<()> {
        let input = r#"
            [Driver]
            shared = "/path/to/dynamic_lib.so"
        "#;
        let manifest = input.parse::<Manifest>()?;
        assert!(manifest.driver().shared().ends_with("dynamic_lib.so"));
        Ok(())
    }

    #[test]
    fn missing_required() -> error::Result<()> {
        assert!(""
            .parse::<Manifest>()
            .is_err_and(|err| err.message.contains("missing field `Driver`")));
        assert!("[Driver]"
            .parse::<Manifest>()
            .is_err_and(|err| err.message.contains("missing field `shared`")));
        Ok(())
    }

    #[test]
    fn invalid_type() -> error::Result<()> {
        assert!("name = 123"
            .parse::<Manifest>()
            .is_err_and(|err| err.message.contains("invalid type: integer")));
        assert!("version = 1"
            .parse::<Manifest>()
            .is_err_and(|err| err.message.contains("expected semver version")));
        Ok(())
    }

    #[test]
    fn all_fields() -> error::Result<()> {
        let input = r#"
            name = "name"
            publisher = "publisher"
            license = "Apache-2.0"
            version = "1.2.3"

            [ADBC]
            version = "1_1_0"

            [ADBC.features]
            supported = ["bulk insert"]
            unsupported = ["async"]

            [Driver]
            shared = "/path/to/dynamic_lib.so"
            entrypoint = "custom_entry_symbol"
        "#;
        let manifest = input.parse::<Manifest>()?;
        assert_eq!(manifest.name(), Some("name"));
        assert_eq!(manifest.publisher(), Some("publisher"));
        assert!(manifest
            .license()
            .map(|license| license
                .evaluate(|req| req.license.id() == spdx::license_id("Apache-2.0")))
            .unwrap_or(false));
        assert_eq!(manifest.version(), Some(&semver::Version::new(1, 2, 3)));
        assert_eq!(manifest.adbc().unwrap().version(), Some(AdbcVersion::V110));
        assert_eq!(
            manifest.adbc().unwrap().features().unwrap().supported(),
            Some(["bulk insert".to_owned()].as_slice())
        );
        assert_eq!(
            manifest.adbc().unwrap().features().unwrap().unsupported(),
            Some(["async".to_owned()].as_slice())
        );
        assert!(manifest.driver().shared().ends_with("dynamic_lib.so"));
        assert_eq!(manifest.driver().entrypoint(), Some("custom_entry_symbol"));
        Ok(())
    }

    #[test]
    fn all_fields_with_arch() -> error::Result<()> {
        let (os, arch, extra) = arch_triplet();
        let input = format!(
            r#"
            name = "name"
            publisher = "publisher"
            license = "Apache-2.0"
            version = "1.2.3"

            [ADBC]
            version = "1_1_0"

            [ADBC.features]
            supported = ["bulk insert"]
            unsupported = ["async"]

            [Driver]
            entrypoint = "custom_entry_symbol"

            [Driver.shared]
            {os}_{arch}{extra} = "/path/to/dynamic_lib.so"
        "#
        );
        let manifest = input.parse::<Manifest>()?;
        assert!(manifest.driver().shared().ends_with("dynamic_lib.so"));
        assert_eq!(manifest.driver().entrypoint(), Some("custom_entry_symbol"));
        Ok(())
    }
}
