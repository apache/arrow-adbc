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

import com.nimbusds.oauth2.sdk.GrantType;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.driver.jdbc.client.oauth.OAuthTokenProvider;
import org.apache.arrow.driver.jdbc.client.oauth.OAuthTokenProviders;

final class FlightSqlOAuthTokenProviders {
  private FlightSqlOAuthTokenProviders() {}

  static OAuthTokenProvider create(FlightSqlOAuthConfiguration configuration)
      throws AdbcException {
    final GrantType flowType = configuration.flowType();
    if (GrantType.CLIENT_CREDENTIALS.equals(flowType)) {
      return createClientCredentialsProvider(configuration);
    }
    if (GrantType.TOKEN_EXCHANGE.equals(flowType)) {
      return createTokenExchangeProvider(configuration);
    }
    throw AdbcException.notImplemented("[Flight SQL] oauth flow not implemented: " + flowType);
  }

  private static OAuthTokenProvider createClientCredentialsProvider(
      FlightSqlOAuthConfiguration configuration) throws AdbcException {
    try {
      final OAuthTokenProviders.ClientCredentialsBuilder builder =
          OAuthTokenProviders.clientCredentials()
              .tokenUri(Objects.requireNonNull(configuration.tokenUri()))
              .clientId(Objects.requireNonNull(configuration.clientId()))
              .clientSecret(Objects.requireNonNull(configuration.clientSecret()));

      final String scope = configuration.scope();
      if (scope != null) {
        builder.scope(scope);
      }
      return builder.build();
    } catch (RuntimeException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Invalid OAuth client credentials configuration: " + e.getMessage())
          .withCause(e);
    }
  }

  private static OAuthTokenProvider createTokenExchangeProvider(
      FlightSqlOAuthConfiguration configuration) throws AdbcException {
    try {
      final OAuthTokenProviders.TokenExchangeBuilder builder =
          OAuthTokenProviders.tokenExchange()
              .tokenUri(Objects.requireNonNull(configuration.tokenUri()))
              .subjectToken(Objects.requireNonNull(configuration.subjectToken()))
              .subjectTokenType(
                  Objects.requireNonNull(configuration.subjectTokenType()).toString());

      if (configuration.actorToken() != null) {
        builder
            .actorToken(configuration.actorToken())
            .actorTokenType(Objects.requireNonNull(configuration.actorTokenType()).toString());
      }
      if (configuration.clientId() != null) {
        builder.clientCredentials(
            configuration.clientId(), Objects.requireNonNull(configuration.clientSecret()));
      }
      if (configuration.audience() != null) {
        builder.audience(configuration.audience());
      }
      if (configuration.requestedTokenType() != null) {
        builder.requestedTokenType(configuration.requestedTokenType().toString());
      }
      if (configuration.resource() != null) {
        builder.resource(configuration.resource());
      }
      if (configuration.scope() != null) {
        builder.scope(configuration.scope());
      }

      return builder.build();
    } catch (RuntimeException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Invalid OAuth token exchange configuration: " + e.getMessage())
          .withCause(e);
    }
  }
}
