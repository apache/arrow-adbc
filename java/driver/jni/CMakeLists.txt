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

add_custom_command(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/target/headers/org_apache_arrow_adbc_driver_jni_impl_NativeAdbc.h
                   COMMENT "Generate JNI headers"
                   # Force Maven to actually re-run the command
                   COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/target/headers
                           ${CMAKE_CURRENT_SOURCE_DIR}/target/maven-status
                   COMMAND mvn --file ${CMAKE_CURRENT_SOURCE_DIR}/../.. -Pjni,javah
                           compile --also-make --projects :adbc-driver-jni
                   DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/main/java/org/apache/arrow/adbc/driver/jni/impl/NativeAdbc.java
)

add_library(adbc_driver_jni SHARED
            src/main/cpp/jni_wrapper.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/target/headers/org_apache_arrow_adbc_driver_jni_impl_NativeAdbc.h
)
target_include_directories(adbc_driver_jni
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/target/headers)
target_link_libraries(adbc_driver_jni JNI::JNI
                      AdbcDriverManager::adbc_driver_manager_static)

set(ADBC_DRIVER_JNI_C_LIBDIR "adbc_driver_jni/${ADBC_ARCH_DIR}")
set(ADBC_DRIVER_JNI_C_BINDIR "adbc_driver_jni/${ADBC_ARCH_DIR}")

install(TARGETS adbc_driver_jni
        LIBRARY DESTINATION ${ADBC_DRIVER_JNI_C_LIBDIR}
        RUNTIME DESTINATION ${ADBC_DRIVER_JNI_C_BINDIR})
