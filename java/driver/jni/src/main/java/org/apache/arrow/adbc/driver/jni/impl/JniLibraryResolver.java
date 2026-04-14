/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adbc.driver.jni.impl;

import java.io.File;

/**
 * Resolves the JNI native library path from the {@code arrow.adbc.driver.jni.library.path} system
 * property. Separated from {@link JniLoader} so that it can be tested without triggering native
 * library loading.
 */
class JniLibraryResolver {

  static final String LIBRARY_NAME = "adbc_driver_jni";
  static final String LIBRARY_PATH_PROPERTY = "arrow.adbc.driver.jni.library.path";

  private JniLibraryResolver() {}

  /**
   * Resolve the native library path from the {@code arrow.adbc.driver.jni.library.path} system
   * property. Returns the absolute path to load, or {@code null} if the property is not set or the
   * library file does not exist at the specified location.
   */
  static String resolveLibraryPath(String libraryName) {
    String libraryPath = System.getProperty(LIBRARY_PATH_PROPERTY);
    if (libraryPath != null) {
      File libraryFile = new File(libraryPath, System.mapLibraryName(libraryName));
      if (libraryFile.isFile()) {
        return libraryFile.getAbsolutePath();
      }
    }
    return null;
  }
}
