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
package org.apache.arrow.adbc.core;

import java.util.Arrays;

/**
 * An ADBC resource which can be closed.
 */
public interface AdbcCloseable extends AutoCloseable {
    @Override
    void close() throws AdbcException;

    /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one.
   * @param autoCloseables the closeables to close
   */
  public static void close(AdbcCloseable... adbcCloseables) throws AdbcException {
    close(Arrays.asList(adbcCloseables));
  }

  /**
   * Closes all {@link AdbcCloseable} instances if not null and suppresses subsequent exceptions if more than one.
   * @param ac the closeables to close
   */
  public static void close(Iterable<? extends AdbcCloseable> ac) throws AdbcException {
    // this method can be called on a single object if it implements Iterable<AutoCloseable>
    // like for example VectorContainer make sure we handle that properly
    if (ac == null) {
      return;
    } else if (ac instanceof AdbcCloseable) {
      ((AdbcCloseable) ac).close();
      return;
    }

    AdbcException topLevelException = null;
    for (AdbcCloseable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (AdbcException e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else if (e != topLevelException) {
          topLevelException.addSuppressed(e);
        }
      }
    }
    if (topLevelException != null) {
      throw topLevelException;
    }
  }
}
