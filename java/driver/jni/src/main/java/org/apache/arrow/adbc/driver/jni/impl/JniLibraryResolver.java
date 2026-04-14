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
import java.util.Locale;

/** Resolves the JNI native library location. */
class JniLibraryResolver {

  static final String PROPERTY = "arrow.adbc.driver.jni.library.path";
  private static final String LIBRARY_NAME = "adbc_driver_jni";

  /** Returns absolute file path if the system property points to an existing library, else null. */
  static String resolve() {
    String dir = System.getProperty(PROPERTY);
    if (dir == null) {
      return null;
    }
    File file = new File(dir, System.mapLibraryName(LIBRARY_NAME));
    return file.isFile() ? file.getAbsolutePath() : null;
  }

  /** Returns the platform-specific resource path for JAR extraction. */
  static String resourcePath() {
    return LIBRARY_NAME + "/" + getNormalizedArch() + "/" + System.mapLibraryName(LIBRARY_NAME);
  }

  private static String getNormalizedArch() {
    String arch = System.getProperty("os.arch").toLowerCase(Locale.US);
    switch (arch) {
      case "amd64":
        return "x86_64";
      case "aarch64":
        return "aarch_64";
      default:
        throw new RuntimeException("ADBC JNI driver not supported on architecture " + arch);
    }
  }
}
