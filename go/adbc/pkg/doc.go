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

// Package pkg contains CGO based drivers that can be compiled to
// shared-libraries via go build -buildmode=c-shared.
//
// By implementing it in terms of the adbc interfaces, a template has
// been created to make it very easy to generate new drivers. These
// templates are in the _tmpl folder. A mainprog defined in ./gen can
// be utilized to generate a new driver by providing a function prefix
// and the path to the driver package.
//
// These generations are added here using go generate to make it easy to
// generate all drivers via a single `go generate` command.
package pkg

//go:generate go run ./gen -prefix "FlightSQL" -driver ../driver/flightsql -o flightsql
//go:generate go run ./gen -prefix "Snowflake" -driver ../driver/snowflake -o snowflake
//go:generate go run ./gen -prefix "PanicDummy" -driver ../driver/panicdummy -o panicdummy
