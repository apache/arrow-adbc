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

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.testsuite.SqlValidationQuirks;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assumptions;

public class FlightSqlQuirks extends SqlValidationQuirks {
  static final String FLIGHT_SQL_LOCATION_ENV_VAR = "ADBC_FLIGHT_SQL_LOCATION";

  static String getFlightLocation() {
    final String location = System.getenv(FLIGHT_SQL_LOCATION_ENV_VAR);
    Assumptions.assumeFalse(
        location == null || location.isEmpty(),
        "Flight SQL server not found, set " + FLIGHT_SQL_LOCATION_ENV_VAR);
    return location;
  }

  @Override
  public AdbcDatabase initDatabase() throws AdbcException {
    String url = getFlightLocation();

    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(AdbcDriver.PARAM_URL, url);
    return FlightSqlDriver.INSTANCE.open(parameters);
  }

  @Override
  public void cleanupTable(String name) throws Exception {
    try (final BufferAllocator allocator = new RootAllocator();
        final FlightSqlClient client =
            new FlightSqlClient(
                FlightClient.builder(allocator, new Location(getFlightLocation())).build())) {
      client.executeUpdate("DROP TABLE " + name);
    } catch (FlightRuntimeException e) {
      // Ignored
    }
  }

  @Override
  public String caseFoldTableName(String name) {
    return name.toUpperCase();
  }

  @Override
  public String caseFoldColumnName(String name) {
    return name.toUpperCase();
  }
}
