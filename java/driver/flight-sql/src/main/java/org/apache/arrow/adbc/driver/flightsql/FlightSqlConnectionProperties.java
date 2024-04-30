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

import java.io.InputStream;
import org.apache.arrow.adbc.core.TypedKey;

/** Defines connection options that are used by the FlightSql driver. */
public interface FlightSqlConnectionProperties {
  TypedKey<InputStream> MTLS_CERT_CHAIN =
      new TypedKey<>("adbc.flight.sql.client_option.mtls_cert_chain", InputStream.class);
  TypedKey<InputStream> MTLS_PRIVATE_KEY =
      new TypedKey<>("adbc.flight.sql.client_option.mtls_private_key", InputStream.class);
  TypedKey<String> TLS_OVERRIDE_HOSTNAME =
      new TypedKey<>("adbc.flight.sql.client_option.tls_override_hostname", String.class);
  TypedKey<Boolean> TLS_SKIP_VERIFY =
      new TypedKey<>("adbc.flight.sql.client_option.tls_skip_verify", Boolean.class);
  TypedKey<InputStream> TLS_ROOT_CERTS =
      new TypedKey<>("adbc.flight.sql.client_option.tls_root_certs", InputStream.class);
  TypedKey<Boolean> WITH_COOKIE_MIDDLEWARE =
      new TypedKey<>("adbc.flight.sql.rpc.with_cookie_middleware", Boolean.class);
  String RPC_CALL_HEADER_PREFIX = "adbc.flight.sql.rpc.call_header.";
}
