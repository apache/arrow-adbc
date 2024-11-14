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

# Common definitions for the CMake projects in this repository.
# Must define REPOSITORY_ROOT before including this.

# ------------------------------------------------------------
# Version definitions

set(ADBC_VERSION "1.4.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ADBC_BASE_VERSION "${ADBC_VERSION}")
string(REPLACE "." ";" _adbc_version_list "${ADBC_BASE_VERSION}")
list(GET _adbc_version_list 0 ADBC_VERSION_MAJOR)
list(GET _adbc_version_list 1 ADBC_VERSION_MINOR)
list(GET _adbc_version_list 2 ADBC_VERSION_PATCH)

math(EXPR ADBC_SO_VERSION "${ADBC_VERSION_MAJOR} * 100 + ${ADBC_VERSION_MINOR}")
set(ADBC_FULL_SO_VERSION "${ADBC_SO_VERSION}.${ADBC_VERSION_PATCH}.0")
