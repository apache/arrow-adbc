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

#pragma once

#include <string>

#include "arrow-adbc/adbc.h"

#if defined(_WIN32)
#define ADBC_LITTLE_ENDIAN 1
#else
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <machine/endian.h>
#elif defined(sun) || defined(__sun)
#include <sys/byteorder.h>
#elif !defined(_AIX)
#include <endian.h>
#endif
#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__)
#define ADBC_LITTLE_ENDIAN 1
#else
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define ADBC_LITTLE_ENDIAN 1
#else
#define ADBC_LITTLE_ENDIAN 0
#endif
#endif
#endif

ADBC_EXPORT
const std::string& InternalAdbcCurrentArch();
