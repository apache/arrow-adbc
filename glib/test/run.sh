#!/usr/bin/env bash
#
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

test_dir="$(cd $(dirname $0); pwd)"
build_dir="$(cd .; pwd)"

modules=(
  adbc-glib
)

for module in "${modules[@]}"; do
  module_build_dir="${build_dir}/${module}"
  if [ -d "${module_build_dir}" ]; then
    DYLD_LIBRARY_PATH="${module_build_dir}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
    LD_LIBRARY_PATH="${module_build_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  fi
done
export DYLD_LIBRARY_PATH
export LD_LIBRARY_PATH

if [ "${BUILD}" != "no" ]; then
  if [ -f "build.ninja" ]; then
    ninja || exit $?
  fi
fi

for module in "${modules[@]}"; do
  MODULE_TYPELIB_DIR_VAR_NAME="$(echo ${module} | tr a-z- A-Z_)_TYPELIB_DIR"
  module_typelib_dir=$(eval "echo \${${MODULE_TYPELIB_DIR_VAR_NAME}}")
  if [ -z "${module_typelib_dir}" ]; then
    module_typelib_dir="${build_dir}/${module}"
  fi

  if [ -d "${module_typelib_dir}" ]; then
    GI_TYPELIB_PATH="${module_typelib_dir}${GI_TYPELIB_PATH:+:${GI_TYPELIB_PATH}}"
  fi
done
export GI_TYPELIB_PATH

${GDB} ruby ${test_dir}/run.rb "$@"
