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

package org.apache.arrow.adbc.driver.flightsql;

import org.apache.arrow.adbc.driver.testsuite.AbstractTransactionTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

public class FlightSqlTransactionTest extends AbstractTransactionTest {

  @BeforeAll
  public static void beforeAll() {
    quirks = new FlightSqlQuirks();
  }

  @Override
  @Disabled("Not yet implemented")
  public void enableAutoCommitAlsoCommits() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void commit() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void rollback() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void autoCommitByDefault() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void toggleAutoCommit() throws Exception {}
}
