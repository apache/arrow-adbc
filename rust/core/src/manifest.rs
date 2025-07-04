//! Driver manifest.

use std::{
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{de, Deserialize, Deserializer};

use crate::{
    error::{Error, Status},
    options::AdbcVersion,
};

/// A driver manifest
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Manifest {
    /// Driver display name
    name: Option<String>,

    /// Publisher identifier
    publisher: Option<String>,

    /// Driver license
    #[serde(default, deserialize_with = "de_from_str_opt")]
    license: Option<spdx::Expression>,

    /// Driver version
    version: Option<semver::Version>,

    /// ADBC
    #[serde(rename = "ADBC")]
    adbc: Option<ADBC>,

    /// Driver
    #[serde(rename = "Driver")]
    driver: Driver,
}

impl Manifest {
    /// Returns the driver display name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the publisher identifier.
    pub fn publisher(&self) -> Option<&str> {
        self.publisher.as_deref()
    }

    /// Returns the license.
    pub fn license(&self) -> Option<&spdx::Expression> {
        self.license.as_ref()
    }

    /// Returns the driver version.
    pub fn version(&self) -> Option<&semver::Version> {
        self.version.as_ref()
    }

    // Returns the ADBC field of the manifest.
    pub fn adbc(&self) -> Option<&ADBC> {
        self.adbc.as_ref()
    }

    // Returns the Driver field of the manifest.
    pub fn driver(&self) -> &Driver {
        &self.driver
    }
}

/// Deserialize an `Option<T>` using the [`FromStr`] impl of `T`.
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

/// ADBC information
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ADBC {
    /// Supported ADBC Spec version
    #[serde(deserialize_with = "de_from_str_opt")]
    version: Option<AdbcVersion>,

    /// Features
    features: Option<Features>,
}

impl ADBC {
    /// Returns the supported ADBC Spec version.
    pub fn version(&self) -> Option<AdbcVersion> {
        self.version
    }

    /// Returns the `features`.
    pub fn features(&self) -> Option<&Features> {
        self.features.as_ref()
    }
}

/// ADBC features
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Features {
    /// List of supported features
    supported: Option<Vec<String>>,

    /// List of unsupported features
    unsupported: Option<Vec<String>>,
}

impl Features {
    /// Returns supported features.
    pub fn supported(&self) -> Option<&[String]> {
        self.supported.as_deref()
    }

    /// Returns unsupported features.
    pub fn unsupported(&self) -> Option<&[String]> {
        self.unsupported.as_deref()
    }
}

/// Driver information
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Driver {
    /// Path to shared library that should be loaded for this driver
    shared: PathBuf,

    /// Entrypoint symbol that overrides the default init symbol (`AdbcDriverInit`)
    entrypoint: Option<String>,
}

impl Driver {
    /// Returns the path to the shared library for this driver.
    pub fn shared(&self) -> &Path {
        self.shared.as_path()
    }

    /// Returns the custom entrypoint.
    pub fn entrypoint(&self) -> Option<&str> {
        self.entrypoint.as_deref()
    }
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
        assert_eq!(
            manifest.adbc().and_then(ADBC::version),
            Some(AdbcVersion::V110)
        );
        assert_eq!(
            manifest
                .adbc()
                .and_then(ADBC::features)
                .and_then(Features::supported),
            Some(["bulk insert".to_owned()].as_slice())
        );
        assert_eq!(
            manifest
                .adbc()
                .and_then(ADBC::features)
                .and_then(Features::unsupported),
            Some(["async".to_owned()].as_slice())
        );
        Ok(())
    }
}
