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

//! Builder utilities
//!
//!

use std::iter::{Chain, Flatten};
#[cfg(feature = "env")]
use std::{env, error::Error as StdError};

#[cfg(feature = "env")]
use adbc_core::error::{Error, Status};
use adbc_core::options::OptionValue;

/// An iterator over the builder options.
pub struct BuilderIter<T, const COUNT: usize>(
    #[allow(clippy::type_complexity)]
    Chain<
        Flatten<<[Option<(T, OptionValue)>; COUNT] as IntoIterator>::IntoIter>,
        <Vec<(T, OptionValue)> as IntoIterator>::IntoIter,
    >,
);

impl<T, const COUNT: usize> BuilderIter<T, COUNT> {
    pub(crate) fn new(
        fixed: [Option<(T, OptionValue)>; COUNT],
        other: Vec<(T, OptionValue)>,
    ) -> Self {
        Self(fixed.into_iter().flatten().chain(other))
    }
}

impl<T, const COUNT: usize> Iterator for BuilderIter<T, COUNT> {
    type Item = (T, OptionValue);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[cfg(feature = "env")]
/// Attempt to read the environment variable with the given `key`, parsing it
/// using the provided `parse` function.
///
/// Returns
///
/// - `Ok(None)` when the env variable is not set.
/// - `Ok(Some(T))` when the env variable is set and the parser succeeds.
/// - `Err(Error)` when the env variable is set and the parse fails.
pub(crate) fn env_parse<T>(
    key: &str,
    parse: impl FnOnce(&str) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    env::var(key).ok().as_deref().map(parse).transpose()
}

#[cfg(feature = "env")]
/// Attempt to read the environment variable with the given `key`, parsing it
/// using the provided `parse` function, mapping the parse result to an
/// [`Error`] with [`Status::InvalidArguments`].
pub(crate) fn env_parse_map_err<T, E: StdError>(
    key: &str,
    parse: impl FnOnce(&str) -> Result<T, E>,
) -> Result<Option<T>, Error> {
    env::var(key)
        .ok()
        .as_deref()
        .map(parse)
        .transpose()
        .map_err(|err| Error::with_message_and_status(err.to_string(), Status::InvalidArguments))
}
