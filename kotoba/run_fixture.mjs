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

import fs from "node:fs";

const path = process.argv[2];
if (!path) {
  console.error("usage: node kotoba/run_fixture.mjs <module.wasm>");
  process.exit(2);
}

const bytes = fs.readFileSync(path);
const { instance } = await WebAssembly.instantiate(bytes);
if (typeof instance.exports.main !== "function") {
  console.error(`${path}: missing exported main`);
  process.exit(1);
}

const result = instance.exports.main();
const value = typeof result === "bigint" ? result : BigInt(result);
if (value !== 0n) {
  console.error(`${path}: fixture failed main=${value}`);
  process.exit(1);
}

console.log(`fixture ok ${path}`);
