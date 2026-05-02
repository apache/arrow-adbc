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

package org.apache.arrow.adbc.driver.flightsql.oauth;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.driver.jdbc.client.oauth.OAuthTokenProvider;
import org.apache.arrow.flight.CallHeaders;

public final class FlightSqlOAuthCredentialWriter implements Consumer<CallHeaders> {
  private final OAuthTokenProvider tokenProvider;

  private FlightSqlOAuthCredentialWriter(OAuthTokenProvider tokenProvider) {
    this.tokenProvider = tokenProvider;
  }

  public static FlightSqlOAuthCredentialWriter create(Map<String, Object> parameters)
      throws AdbcException {
    final FlightSqlOAuthConfiguration configuration =
        FlightSqlOAuthConfiguration.from(parameters);
    return new FlightSqlOAuthCredentialWriter(FlightSqlOAuthTokenProviders.create(configuration));
  }

  public void prefetchToken() throws AdbcException {
    currentAuthorizationValue();
  }

  @Override
  public void accept(CallHeaders headers) {
    try {
      headers.insert("authorization", currentAuthorizationValue());
    } catch (AdbcException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  private String currentAuthorizationValue() throws AdbcException {
    try {
      return "Bearer " + tokenProvider.getValidToken();
    } catch (SQLException e) {
      throw AdbcException.io("[Flight SQL] OAuth token request failed: " + e.getMessage())
          .withCause(e);
    }
  }
}
