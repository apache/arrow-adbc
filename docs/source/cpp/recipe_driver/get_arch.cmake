# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if(WIN32)
  set(OS "windows")
elseif(APPLE)
  set(OS "osx")
else()
  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    set(OS "freebsd")
  else()
    set(OS "linux")
  endif()
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
  set(ARCH "amd64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(i386|i486|i586|i686)$")
  set(ARCH "x86")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(armv8b|armv8l|aarch64|arm64|AARCH64|ARM64)$")
  set(ARCH "arm64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(armv7l|armhf|arm)$")
  set(ARCH "arm")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ppc|ppc64|ppcle|ppc64le)$")
  set(ARCH "powerpc")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "riscv64")
  set(ARCH "riscv")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(s390|s390x)$")
  set(ARCH "s390x")
else()
  message(FATAL_ERROR "Unsupported architecture: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

include(CheckCSourceRuns)

check_c_source_runs([=[
#include <stdlib.h>
#if defined(__GLIBC__)
#error "GLIBC detected"
#elif defined(__MUSL__)
int main(void) { return EXIT_SUCCESS; }
#else
#error "Neither GLIBC nor Musl detected"
#endif
  ]=]
                    IS_MUSL)

if(MINGW)
  set(EXTRA "_mingw")
elseif(IS_MUSL)
  set(EXTRA "_musl")
endif()
