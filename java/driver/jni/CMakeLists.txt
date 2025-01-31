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

find_package(AdbcDriverManager)

add_jar(adbc_driver_jni_jar
        SOURCES src/main/java/org/apache/arrow/adbc/driver/jni/impl/NativeAdbc.java
                src/main/java/org/apache/arrow/adbc/driver/jni/impl/NativeAdbcException.java
                src/main/java/org/apache/arrow/adbc/driver/jni/impl/NativeHandle.java
                src/main/java/org/apache/arrow/adbc/driver/jni/impl/NativeQueryResult.java
                GENERATE_NATIVE_HEADERS
                adbc_driver_jni_headers)

add_library(adbc_driver_jni SHARED src/main/cpp/jni_wrapper.cc)
target_link_libraries(adbc_driver_jni adbc_driver_jni_headers JNI::JNI
                      AdbcDriverManager::adbc_driver_manager_static)

set(ADBC_DRIVER_JNI_C_LIBDIR "adbc_driver_jni/${ADBC_ARCH_DIR}")
set(ADBC_DRIVER_JNI_C_BINDIR "adbc_driver_jni/${ADBC_ARCH_DIR}")

install(TARGETS adbc_driver_jni
        LIBRARY DESTINATION ${ADBC_DRIVER_JNI_C_LIBDIR}
        RUNTIME DESTINATION ${ADBC_DRIVER_JNI_C_BINDIR})
