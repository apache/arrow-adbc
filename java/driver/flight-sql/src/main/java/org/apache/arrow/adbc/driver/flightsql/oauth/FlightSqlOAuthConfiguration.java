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
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.checkerframework.checker.nullness.qual.Nullable;

final class FlightSqlOAuthConfiguration {
  private final GrantType flowType;
  private final @Nullable String tokenUri;
  private final @Nullable String clientId;
  private final @Nullable String clientSecret;
  private final @Nullable String scope;
  private final @Nullable String subjectToken;
  private final @Nullable TokenTypeURI subjectTokenType;
  private final @Nullable String actorToken;
  private final @Nullable TokenTypeURI actorTokenType;
  private final @Nullable TokenTypeURI requestedTokenType;
  private final @Nullable String audience;
  private final @Nullable URI resource;

  private FlightSqlOAuthConfiguration(
      GrantType flowType,
      @Nullable String tokenUri,
      @Nullable String clientId,
      @Nullable String clientSecret,
      @Nullable String scope,
      @Nullable String subjectToken,
      @Nullable TokenTypeURI subjectTokenType,
      @Nullable String actorToken,
      @Nullable TokenTypeURI actorTokenType,
      @Nullable TokenTypeURI requestedTokenType,
      @Nullable String audience,
      @Nullable URI resource) {
    this.flowType = flowType;
    this.tokenUri = tokenUri;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = scope;
    this.subjectToken = subjectToken;
    this.subjectTokenType = subjectTokenType;
    this.actorToken = actorToken;
    this.actorTokenType = actorTokenType;
    this.requestedTokenType = requestedTokenType;
    this.audience = audience;
    this.resource = resource;
  }

  static FlightSqlOAuthConfiguration from(Map<String, Object> parameters) throws AdbcException {
    final GrantType flowType =
        parseGrantType(requireOption(parameters, FlightSqlConnectionProperties.OAUTH_FLOW));
    final @Nullable String clientId = FlightSqlConnectionProperties.OAUTH_CLIENT_ID.get(parameters);
    final @Nullable String clientSecret =
        FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.get(parameters);
    final @Nullable String subjectToken =
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN.get(parameters);
    final @Nullable String subjectTokenTypeValue =
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.get(parameters);
    final @Nullable String actorToken =
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN.get(parameters);
    final @Nullable String actorTokenTypeValue =
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.get(parameters);

    final boolean clientCredentials = GrantType.CLIENT_CREDENTIALS.equals(flowType);
    final boolean tokenExchange = GrantType.TOKEN_EXCHANGE.equals(flowType);
    final @Nullable String tokenUri =
        (clientCredentials || tokenExchange)
            ? requireOption(parameters, FlightSqlConnectionProperties.OAUTH_TOKEN_URI)
            : null;

    if (clientCredentials) {
      requireOption(parameters, FlightSqlConnectionProperties.OAUTH_CLIENT_ID);
      requireOption(parameters, FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET);
    }
    if (tokenExchange) {
      requireOption(parameters, FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN);
      requireOption(parameters, FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE);
      if ((actorToken == null) != (actorTokenTypeValue == null)) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] token exchange grant requires "
                + FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.getKey()
                + " when "
                + FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN.getKey()
                + " is provided");
      }
      if ((clientId == null) != (clientSecret == null)) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] token exchange grant requires both "
                + FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey()
                + " and "
                + FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey()
                + " when client credentials are provided");
      }
    }

    final @Nullable String resource =
        tokenExchange ? FlightSqlConnectionProperties.OAUTH_RESOURCE.get(parameters) : null;
    return new FlightSqlOAuthConfiguration(
        flowType,
        tokenUri,
        clientId,
        clientSecret,
        FlightSqlConnectionProperties.OAUTH_SCOPE.get(parameters),
        subjectToken,
        parseTokenTypeUri(
            subjectTokenTypeValue,
            FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE),
        actorToken,
        parseTokenTypeUri(
            FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.get(parameters),
            FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE),
        parseTokenTypeUri(
            FlightSqlConnectionProperties.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE.get(parameters),
            FlightSqlConnectionProperties.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE),
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_AUD.get(parameters),
        parseResource(resource));
  }

  GrantType flowType() {
    return flowType;
  }

  @Nullable String tokenUri() {
    return tokenUri;
  }

  @Nullable String clientId() {
    return clientId;
  }

  @Nullable String clientSecret() {
    return clientSecret;
  }

  @Nullable String scope() {
    return scope;
  }

  @Nullable String subjectToken() {
    return subjectToken;
  }

  @Nullable TokenTypeURI subjectTokenType() {
    return subjectTokenType;
  }

  @Nullable String actorToken() {
    return actorToken;
  }

  @Nullable TokenTypeURI actorTokenType() {
    return actorTokenType;
  }

  @Nullable TokenTypeURI requestedTokenType() {
    return requestedTokenType;
  }

  @Nullable String audience() {
    return audience;
  }

  @Nullable URI resource() {
    return resource;
  }

  private static String requireOption(Map<String, Object> parameters, TypedKey<String> option)
      throws AdbcException {
    final String value = option.get(parameters);
    if (value == null) {
      throw AdbcException.invalidArgument("[Flight SQL] OAuth flow requires " + option.getKey());
    }
    return value;
  }

  private static GrantType parseGrantType(String value) throws AdbcException {
    try {
      return GrantType.parse(value);
    } catch (ParseException e) {
      throw AdbcException.invalidArgument("[Flight SQL] invalid OAuth flow: " + value).withCause(e);
    }
  }

  private static @Nullable TokenTypeURI parseTokenTypeUri(
      @Nullable String value, TypedKey<String> option) throws AdbcException {
    if (value == null) {
      return null;
    }
    try {
      return TokenTypeURI.parse(value);
    } catch (ParseException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] invalid OAuth token type for " + option.getKey() + ": " + value)
          .withCause(e);
    }
  }

  private static @Nullable URI parseResource(@Nullable String resource) throws AdbcException {
    if (resource == null) {
      return null;
    }
    try {
      return new URI(resource);
    } catch (URISyntaxException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] token exchange grant requires a valid URI for "
                  + FlightSqlConnectionProperties.OAUTH_RESOURCE.getKey())
          .withCause(e);
    }
  }
}
