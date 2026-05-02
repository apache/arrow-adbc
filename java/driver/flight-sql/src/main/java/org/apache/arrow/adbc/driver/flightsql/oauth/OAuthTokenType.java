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

public enum OAuthTokenType {
  ACCESS_TOKEN("urn:ietf:params:oauth:token-type:access_token"),
  REFRESH_TOKEN("urn:ietf:params:oauth:token-type:refresh_token"),
  JWT("urn:ietf:params:oauth:token-type:jwt"),
  ID_TOKEN("urn:ietf:params:oauth:token-type:id_token"),
  SAML1("urn:ietf:params:oauth:token-type:saml1"),
  SAML2("urn:ietf:params:oauth:token-type:saml2");

  private final String value;

  OAuthTokenType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
