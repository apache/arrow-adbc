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

//! Builder utility.
//!

use std::iter::{Chain, Flatten};

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
