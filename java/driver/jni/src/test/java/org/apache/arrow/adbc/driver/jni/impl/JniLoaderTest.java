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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JniLoaderTest {

  @AfterEach
  void clearProperty() {
    System.clearProperty(JniLoader.LIBRARY_PATH_PROPERTY);
  }

  @Test
  void resolveLibraryPathPropertyNotSet() {
    assertThat(JniLoader.resolveLibraryPath(JniLoader.LIBRARY_NAME)).isNull();
  }

  @Test
  void resolveLibraryPathFileExists(@TempDir Path tempDir) throws IOException {
    String libraryFileName = System.mapLibraryName(JniLoader.LIBRARY_NAME);
    File libraryFile = tempDir.resolve(libraryFileName).toFile();
    assertThat(libraryFile.createNewFile()).isTrue();

    System.setProperty(JniLoader.LIBRARY_PATH_PROPERTY, tempDir.toString());

    String resolved = JniLoader.resolveLibraryPath(JniLoader.LIBRARY_NAME);
    assertThat(resolved).isEqualTo(libraryFile.getAbsolutePath());
  }

  @Test
  void resolveLibraryPathFileMissing(@TempDir Path tempDir) {
    System.setProperty(JniLoader.LIBRARY_PATH_PROPERTY, tempDir.toString());

    assertThat(JniLoader.resolveLibraryPath(JniLoader.LIBRARY_NAME)).isNull();
  }

  @Test
  void resolveLibraryPathDirectoryDoesNotExist() {
    System.setProperty(JniLoader.LIBRARY_PATH_PROPERTY, "/nonexistent/path");

    assertThat(JniLoader.resolveLibraryPath(JniLoader.LIBRARY_NAME)).isNull();
  }
}
