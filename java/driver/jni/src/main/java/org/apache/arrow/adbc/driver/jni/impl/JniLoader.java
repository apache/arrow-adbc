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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.util.Preconditions;

public enum JniLoader {
  INSTANCE;

  JniLoader() {
    final String libraryName = "adbc_driver_jni";
    String libraryToLoad =
        libraryName + "/" + getNormalizedArch() + "/" + System.mapLibraryName(libraryName);

    try {
      InputStream is = JniLoader.class.getClassLoader().getResourceAsStream(libraryToLoad);
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      }
      File temp =
          File.createTempFile("adbc-jni-", ".tmp", new File(System.getProperty("java.io.tmpdir")));
      temp.deleteOnExit();

      try (is) {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      Runtime.getRuntime().load(temp.getAbsolutePath());
    } catch (IOException e) {
      throw new IllegalStateException("Error loading native library: " + e);
    }
  }

  private String getNormalizedArch() {
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

  public NativeHandle openDatabase(Map<String, String> parameters) {
    String[] nativeParameters = new String[parameters.size() * 2];
    int index = 0;
    for (Map.Entry<String, String> parameter : parameters.entrySet()) {
      nativeParameters[index++] = parameter.getKey();
      nativeParameters[index++] = parameter.getValue();
    }
    try {
      return NativeAdbc.openDatabase(1001000, nativeParameters);
    } catch (NativeAdbcException e) {
      // TODO: convert to AdbcException
      throw new RuntimeException(e);
    }
  }

  public NativeHandle openConnection(NativeHandle database) {
    Preconditions.checkArgument(database.getHandleType() == NativeHandleType.DATABASE);
    try {
      return NativeAdbc.openConnection(database.getHandle());
    } catch (NativeAdbcException e) {
      // TODO: convert to AdbcException
      throw new RuntimeException(e);
    }
  }

  public NativeHandle openStatement(NativeHandle connection) {
    Preconditions.checkArgument(connection.getHandleType() == NativeHandleType.CONNECTION);
    try {
      return NativeAdbc.openStatement(connection.getHandle());
    } catch (NativeAdbcException e) {
      // TODO: convert to AdbcException
      throw new RuntimeException(e);
    }
  }

  public NativeQueryResult statementExecuteQuery(NativeHandle statement) throws AdbcException {
    try {
      return NativeAdbc.statementExecuteQuery(statement.getHandle());
    } catch (NativeAdbcException e) {
      // TODO: convert to AdbcException
      throw new RuntimeException(e);
    }
  }

  public void statementSetSqlQuery(NativeHandle statement, String query) throws AdbcException {
    try {
      NativeAdbc.statementSetSqlQuery(statement.getHandle(), query);
    } catch (NativeAdbcException e) {
      // TODO: convert to AdbcException
      throw new RuntimeException(e);
    }
  }
}
