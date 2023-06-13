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

package org.apache.arrow.adbc.driver.jdbc.mssqlserver;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.adbc.driver.testsuite.AbstractSqlTypeTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MsSqlServerSqlTypeTest extends AbstractSqlTypeTest {
  @BeforeAll
  public static void beforeAll() {
    quirks = new MsSqlServerQuirks();
  }

  @Test
  @Override
  protected void timeWithoutTimeZoneValue() {
    // TODO(https://github.com/apache/arrow/issues/35916): needs upstream fix
    // XXX: Java 8 compiler complains without lambda https://stackoverflow.com/questions/33621060
    //noinspection Convert2MethodRef
    assertThrows(AssertionError.class, () -> super.timeWithoutTimeZoneValue());
  }

  @Test
  @Override
  protected void timestampWithoutTimeZoneValue() {
    // TODO(https://github.com/apache/arrow/issues/35916): needs upstream fix
    // XXX: Java 8 compiler complains without lambda https://stackoverflow.com/questions/33621060
    //noinspection Convert2MethodRef
    assertThrows(AssertionError.class, () -> super.timestampWithoutTimeZoneValue());
  }

  @Test
  @Override
  protected void timestampWithTimeZoneValue() {
    // TODO(https://github.com/apache/arrow/issues/35916): needs upstream fix
    // XXX: Java 8 compiler complains without lambda https://stackoverflow.com/questions/33621060
    //noinspection Convert2MethodRef
    assertThrows(RuntimeException.class, () -> super.timestampWithTimeZoneValue());
  }
}
