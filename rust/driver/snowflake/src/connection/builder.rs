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

//! A builder for a [`Connection`]
//!
//!

#[cfg(feature = "env")]
use std::env;
use std::fmt;

use adbc_core::{
    error::Result,
    options::{OptionConnection, OptionValue},
    Database as _,
};

#[cfg(feature = "env")]
use crate::database;
use crate::{builder::BuilderIter, Connection, Database};

/// A builder for [`Connection`].
///
/// The builder can be used to initialize a [`Connection`] with
/// [`Builder::build`] or by directly passing it to
/// [`Database::new_connection_with_opts`].
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct Builder {
    /// Use high precision ([`Self::USE_HIGH_PRECISION`]).
    pub use_high_precision: Option<bool>,

    /// Other options.
    pub other: Vec<(OptionConnection, OptionValue)>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder").field("...", &self.other).finish()
    }
}

#[cfg(feature = "env")]
impl Builder {
    /// See [`Self::use_high_precision`].
    pub const USE_HIGH_PRECISION_ENV: &str = database::Builder::USE_HIGH_PRECISION_ENV;

    /// Construct a builder, setting values based on values of the
    /// configuration environment variables.
    pub fn from_env() -> Self {
        #[cfg(feature = "dotenv")]
        let _ = dotenvy::dotenv();

        let use_high_precision = env::var(Self::USE_HIGH_PRECISION_ENV)
            .ok()
            .as_deref()
            .and_then(|value| value.parse().ok());
        Self {
            use_high_precision,
            ..Default::default()
        }
    }
}

impl Builder {
    /// Number of fields in the builder (except other).
    const COUNT: usize = 1;

    pub const USE_HIGH_PRECISION: &str = "adbc.snowflake.sql.client_option.use_high_precision";

    /// Use high precision ([`Self::use_high_precision`]).
    pub fn with_high_precision(mut self, use_high_precision: bool) -> Self {
        self.use_high_precision = Some(use_high_precision);
        self
    }
}

impl Builder {
    /// Attempt to initialize a [`Connection`] using the values provided to
    /// this builder using the provided [`Database`].
    pub fn build(self, database: &mut Database) -> Result<Connection> {
        database.new_connection_with_opts(self)
    }
}

impl IntoIterator for Builder {
    type Item = (OptionConnection, OptionValue);
    type IntoIter = BuilderIter<OptionConnection, { Builder::COUNT }>;

    fn into_iter(self) -> Self::IntoIter {
        BuilderIter::new(
            [self
                .use_high_precision
                .as_ref()
                .map(ToString::to_string)
                .map(OptionValue::String)
                .map(|value| (Builder::USE_HIGH_PRECISION.into(), value))],
            self.other,
        )
    }
}
