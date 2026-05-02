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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.driver.jdbc.client.oauth.OAuthTokenProvider;
import org.apache.arrow.driver.jdbc.client.oauth.OAuthTokenProviders;
import org.apache.arrow.flight.CallHeaders;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class FlightSqlOAuthCredentialWriter implements Consumer<CallHeaders> {
  private final OAuthTokenProvider tokenProvider;

  private FlightSqlOAuthCredentialWriter(OAuthTokenProvider tokenProvider) {
    this.tokenProvider = tokenProvider;
  }

  public static FlightSqlOAuthCredentialWriter create(Map<String, Object> parameters)
      throws AdbcException {
    final String flowValue = requireOption(parameters, FlightSqlConnectionProperties.OAUTH_FLOW);
    final OAuthFlowType flowType = OAuthFlowType.fromValue(flowValue);
    if (flowType == null) {
      throw AdbcException.notImplemented("[Flight SQL] oauth flow not implemented: " + flowValue);
    }

    switch (flowType) {
      case CLIENT_CREDENTIALS:
        return new FlightSqlOAuthCredentialWriter(createClientCredentialsProvider(parameters));
      case TOKEN_EXCHANGE:
        return new FlightSqlOAuthCredentialWriter(createTokenExchangeProvider(parameters));
      default:
        throw AdbcException.notImplemented("[Flight SQL] oauth flow not implemented: " + flowType);
    }
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

  private static OAuthTokenProvider createClientCredentialsProvider(Map<String, Object> parameters)
      throws AdbcException {
    try {
      final OAuthTokenProviders.ClientCredentialsBuilder builder =
          OAuthTokenProviders.clientCredentials()
              .tokenUri(requireOption(parameters, FlightSqlConnectionProperties.OAUTH_TOKEN_URI))
              .clientId(requireOption(parameters, FlightSqlConnectionProperties.OAUTH_CLIENT_ID))
              .clientSecret(
                  requireOption(parameters, FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET));

      final String scope = FlightSqlConnectionProperties.OAUTH_SCOPE.get(parameters);
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

  private static OAuthTokenProvider createTokenExchangeProvider(Map<String, Object> parameters)
      throws AdbcException {
    try {
      final OAuthTokenProviders.TokenExchangeBuilder builder =
          OAuthTokenProviders.tokenExchange()
              .tokenUri(requireOption(parameters, FlightSqlConnectionProperties.OAUTH_TOKEN_URI))
              .subjectToken(
                  requireOption(
                      parameters, FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN))
              .subjectTokenType(
                  requireOption(
                      parameters,
                      FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE));

      final String actorToken =
          FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN.get(parameters);
      final String actorTokenType =
          FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.get(parameters);
      if ((actorToken == null) != (actorTokenType == null)) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] token exchange grant requires "
                + FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.getKey()
                + " when "
                + FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN.getKey()
                + " is provided");
      }
      if (actorToken != null) {
        builder.actorToken(actorToken).actorTokenType(Objects.requireNonNull(actorTokenType));
      }

      final String clientId = FlightSqlConnectionProperties.OAUTH_CLIENT_ID.get(parameters);
      final String clientSecret =
          FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.get(parameters);
      if ((clientId == null) != (clientSecret == null)) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] token exchange grant requires both "
                + FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey()
                + " and "
                + FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey()
                + " when client credentials are provided");
      }
      if (clientId != null) {
        builder.clientCredentials(clientId, Objects.requireNonNull(clientSecret));
      }

      final String audience = FlightSqlConnectionProperties.OAUTH_EXCHANGE_AUD.get(parameters);
      if (audience != null) {
        builder.audience(audience);
      }

      final String requestedTokenType =
          FlightSqlConnectionProperties.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE.get(parameters);
      if (requestedTokenType != null) {
        builder.requestedTokenType(requestedTokenType);
      }

      final String resource = FlightSqlConnectionProperties.OAUTH_RESOURCE.get(parameters);
      if (resource != null) {
        try {
          builder.resource(new URI(resource));
        } catch (URISyntaxException e) {
          throw AdbcException.invalidArgument(
                  "[Flight SQL] token exchange grant requires a valid URI for "
                      + FlightSqlConnectionProperties.OAUTH_RESOURCE.getKey())
              .withCause(e);
        }
      }

      final @Nullable String scope = FlightSqlConnectionProperties.OAUTH_SCOPE.get(parameters);
      if (scope != null) {
        builder.scope(scope);
      }

      return builder.build();
    } catch (AdbcException e) {
      throw e;
    } catch (RuntimeException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Invalid OAuth token exchange configuration: " + e.getMessage())
          .withCause(e);
    }
  }

  private static String requireOption(Map<String, Object> parameters, TypedKey<String> option)
      throws AdbcException {
    final String value = option.get(parameters);
    if (value == null) {
      throw AdbcException.invalidArgument("[Flight SQL] OAuth flow requires " + option.getKey());
    }
    return value;
  }
}
