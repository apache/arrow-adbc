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

#[cfg(feature = "generate")]
fn main() {
    use bindgen::{callbacks::ParseCallbacks, Builder, Formatter};

    /// Transforms doxygen comments.
    #[derive(Debug)]
    struct Doxygen;

    impl ParseCallbacks for Doxygen {
        fn process_comment(&self, comment: &str) -> Option<String> {
            Some(doxygen_rs::transform(comment))
        }
    }

    Builder::default()
        .header("adbc.h")
        .parse_callbacks(Box::new(Doxygen))
        .formatter(Formatter::Prettyplease)
        .allowlist_item("(?i)adbc.*")
        .generate()
        .expect("failed to generate bindings")
        .write_to_file("adbc.rs")
        .expect("failed to write bindings");

    println!("cargo:rerun-if-changed=adbc.h");
}

#[cfg(not(feature = "generate"))]
fn main() {
    // Prevent rerun
    println!("cargo:rerun-if-changed=build.rs");
}
