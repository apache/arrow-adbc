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

import org.checkerframework.checker.nullness.qual.Nullable;

public enum OAuthFlowType {
  CLIENT_CREDENTIALS("client_credentials"),
  TOKEN_EXCHANGE("token_exchange");

  private final String value;

  OAuthFlowType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  static @Nullable OAuthFlowType fromValue(String value) {
    for (OAuthFlowType flowType : values()) {
      if (flowType.value.equals(value)) {
        return flowType;
      }
    }
    return null;
  }
}
