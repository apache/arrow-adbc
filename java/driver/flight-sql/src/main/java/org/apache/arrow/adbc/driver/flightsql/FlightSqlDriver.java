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

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/** An ADBC driver wrapping Arrow Flight SQL. */
public class FlightSqlDriver implements AdbcDriver {
  private final BufferAllocator allocator;

  public FlightSqlDriver(BufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator);
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    String uri = PARAM_URI.get(parameters);
    if (uri == null) {
      Object target = parameters.get(AdbcDriver.PARAM_URL);
      if (!(target instanceof String)) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Must provide String " + PARAM_URI + " parameter");
      }
      uri = (String) target;
    }

    Location location;
    try {
      location = new Location(uri);
    } catch (URISyntaxException e) {
      throw AdbcException.invalidArgument(
              String.format("[Flight SQL] Location %s is invalid: %s", uri, e))
          .withCause(e);
    }
    Object quirks = parameters.get(PARAM_SQL_QUIRKS);
    if (quirks != null) {
      Preconditions.checkArgument(
          quirks instanceof SqlQuirks,
          String.format(
              "[Flight SQL] %s must be a SqlQuirks instance, not %s",
              PARAM_SQL_QUIRKS, quirks.getClass().getName()));
    } else {
      quirks = new SqlQuirks();
    }
    return new FlightSqlDatabase(allocator, location, (SqlQuirks) quirks, parameters);
  }
}
