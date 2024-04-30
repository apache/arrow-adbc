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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.driver.jdbc.FlightServerTestRule;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.FlightSqlTestCertificates;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MutualTlsTest {

  @ClassRule public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE;

  private static final String USER_1 = "user1";
  private static final String PASS_1 = "pass1";
  private static final String TLS_ROOT_CERTS_PATH;
  private static final String CLIENT_MTLS_CERT_PATH;
  private static final String CLIENT_MTLS_KEY_PATH;

  static {
    final FlightSqlTestCertificates.CertKeyPair certKey =
        FlightSqlTestCertificates.exampleTlsCerts().get(0);

    TLS_ROOT_CERTS_PATH = certKey.cert.getPath();

    final FlightSqlTestCertificates.CertKeyPair clientMTlsCertKey =
        FlightSqlTestCertificates.exampleTlsCerts().get(1);

    CLIENT_MTLS_CERT_PATH = clientMTlsCertKey.cert.getPath();
    CLIENT_MTLS_KEY_PATH = clientMTlsCertKey.key.getPath();

    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user(USER_1, PASS_1).build();

    FLIGHT_SERVER_TEST_RULE =
        new FlightServerTestRule.Builder()
            .authentication(authentication)
            .useEncryption(certKey.cert, certKey.key)
            .useMTlsClientVerification(FlightSqlTestCertificates.exampleCACert())
            .producer(new MockFlightSqlProducer())
            .build();
  }

  private BufferAllocator allocator;
  private Map<String, Object> params;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    params = new HashMap<>();
    params.put(AdbcDriver.PARAM_USERNAME.getKey(), USER_1);
    params.put(AdbcDriver.PARAM_PASSWORD.getKey(), PASS_1);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(allocator);
  }

  @Test
  public void testClientTlsOnVerifyOffServerOnNoCertSpecified() throws Exception {
    params.put(AdbcDriver.PARAM_URI.getKey(), getUri(true));
    params.put(FlightSqlConnectionProperties.TLS_SKIP_VERIFY.getKey(), true);
    AdbcException adbcException =
        assertThrows(
            AdbcException.class,
            () -> {
              AdbcDatabase db =
                  AdbcDriverManager.getInstance()
                      .connect(FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
              try (AdbcConnection conn = db.connect()) {}
            });
    assertEquals(AdbcStatusCode.IO, adbcException.getStatus());
  }

  @Test
  public void testClientTlsOnVerifyOnServerOnNoCertSpecified() throws Exception {
    params.put(AdbcDriver.PARAM_URI.getKey(), getUri(true));
    try (InputStream stream = Files.newInputStream(Paths.get(TLS_ROOT_CERTS_PATH))) {
      params.put(FlightSqlConnectionProperties.TLS_ROOT_CERTS.getKey(), stream);
      AdbcException adbcException =
          assertThrows(
              AdbcException.class,
              () -> {
                AdbcDatabase db =
                    AdbcDriverManager.getInstance()
                        .connect(
                            FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
                try (AdbcConnection conn = db.connect()) {}
              });
      assertEquals(AdbcStatusCode.IO, adbcException.getStatus());
    }
  }

  @Test
  public void testClientTlsOnVerifyOnCertsSpecifiedServerOnNoCertSpecified() throws Exception {
    params.put(AdbcDriver.PARAM_URI.getKey(), getUri(true));
    try (InputStream rootCertStream = Files.newInputStream(Paths.get(TLS_ROOT_CERTS_PATH));
        InputStream privateKeyStream = Files.newInputStream(Paths.get(CLIENT_MTLS_KEY_PATH));
        InputStream clientCertStream = Files.newInputStream(Paths.get(CLIENT_MTLS_CERT_PATH))) {
      params.put(FlightSqlConnectionProperties.TLS_ROOT_CERTS.getKey(), rootCertStream);
      params.put(FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.getKey(), privateKeyStream);
      params.put(FlightSqlConnectionProperties.MTLS_CERT_CHAIN.getKey(), clientCertStream);
      AdbcDatabase db =
          AdbcDriverManager.getInstance()
              .connect(FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
      try (AdbcConnection conn = db.connect()) {}
    }
  }

  private String getUri(boolean withTls) {
    String protocol = String.format("grpc%s", withTls ? "+tls" : "+tcp");
    return String.format(
        "%s://%s:%d",
        protocol, FLIGHT_SERVER_TEST_RULE.getHost(), FLIGHT_SERVER_TEST_RULE.getPort());
  }
}
