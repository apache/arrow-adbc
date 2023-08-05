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

add_library(
  nanoarrow
  STATIC
  nanoarrow.c
)

set_target_properties(
  nanoarrow PROPERTIES POSITION_INDEPENDENT_CODE ON
)

add_library(flatccrt STATIC
  "${REPOSITORY_ROOT}/c/vendor/flatcc/src/runtime/builder.c"
  "${REPOSITORY_ROOT}/c/vendor/flatcc/src/runtime/emitter.c"
  "${REPOSITORY_ROOT}/c/vendor/flatcc/src/runtime/verifier.c"
  "${REPOSITORY_ROOT}/c/vendor/flatcc/src/runtime/refmap.c"
)

# TODO: nanoarrow lets you specify this from command line
set(NANOARROW_IPC_FLATCC_INCLUDE_DIR
  ${REPOSITORY_ROOT}/c/vendor/flatcc/include)

target_include_directories(
  flatccrt PUBLIC $<BUILD_INTERFACE:${NANOARROW_IPC_FLATCC_INCLUDE_DIR}>
  $<INSTALL_INTERFACE:include>)

add_library(
  nanoarrow_ipc
  STATIC
  nanoarrow_ipc.c
)

target_include_directories(nanoarrow_ipc PUBLIC $<BUILD_INTERFACE:${NANOARROW_IPC_FLATCC_INCLUDE_DIR}>)

target_link_libraries(nanoarrow_ipc PRIVATE flatccrt)

set_target_properties(
  nanoarrow_ipc PROPERTIES POSITION_INDEPENDENT_CODE ON
)
