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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OAuthTest {
  private static final String CLIENT_ID = "adbc-client";
  private static final String CLIENT_SECRET = "adbc-secret";
  private static final String CLIENT_ACCESS_TOKEN = "client-credentials-token";
  private static final String EXCHANGE_ACCESS_TOKEN = "token-exchange-token";
  private static final char[] TRUST_STORE_PASSWORD = "changeit".toCharArray();

  private BufferAllocator allocator;
  private Map<String, Object> params;
  private FlightServer server;
  private HttpServer tokenServer;
  private AdbcDatabase database;
  private AdbcConnection connection;
  private HeaderValidator.Factory headerValidatorFactory;
  private TokenHandler tokenHandler;
  private String tokenServerScheme;
  private String previousTrustStore;
  private String previousTrustStorePassword;
  private String previousTrustStoreType;
  private SSLSocketFactory previousDefaultSslSocketFactory;
  private HostnameVerifier previousDefaultHostnameVerifier;
  private final List<Path> tempPaths = new ArrayList<>();

  @BeforeEach
  public void setUp() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    params = new HashMap<>();
    headerValidatorFactory = new HeaderValidator.Factory();
    server =
        FlightServer.builder()
            .allocator(allocator)
            .middleware(HeaderValidator.KEY, headerValidatorFactory)
            .location(Location.forGrpcInsecure("localhost", 0))
            .producer(new MockFlightSqlProducer())
            .build();
    server.start();

    tokenHandler = new TokenHandler();
    previousTrustStore = System.getProperty("javax.net.ssl.trustStore");
    previousTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    previousTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
    previousDefaultSslSocketFactory = HTTPRequest.getDefaultSSLSocketFactory();
    previousDefaultHostnameVerifier = HTTPRequest.getDefaultHostnameVerifier();
    startHttpTokenServer();

    params.put(
        AdbcDriver.PARAM_URI.getKey(), String.format("grpc+tcp://localhost:%d", server.getPort()));
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(connection, database, server, allocator);
    if (tokenServer != null) {
      tokenServer.stop(0);
    }
    restoreJvmTrustStoreConfiguration();
    for (Path path : tempPaths) {
      Files.deleteIfExists(path);
    }
    connection = null;
    database = null;
    server = null;
    allocator = null;
    tokenServer = null;
  }

  @Test
  public void testClientCredentialsFlow() throws Exception {
    tokenHandler.accessToken = CLIENT_ACCESS_TOKEN;
    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(),
        GrantType.CLIENT_CREDENTIALS.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey(), CLIENT_ID);
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey(), CLIENT_SECRET);
    params.put(FlightSqlConnectionProperties.OAUTH_SCOPE.getKey(), "scope-a scope-b");

    connect();
    requestServerMetadata();

    CallHeaders headers = headerValidatorFactory.getHeadersReceivedAtRequest(0);
    assertEquals("Bearer " + CLIENT_ACCESS_TOKEN, headers.get("authorization"));

    assertEquals(1, tokenHandler.requestBodies.size());
    assertEquals("client_credentials", tokenHandler.formValue(0, "grant_type"));
    assertEquals("scope-a scope-b", tokenHandler.formValue(0, "scope"));
    assertTrue(tokenHandler.authorizationHeaders.get(0).startsWith("Basic "));
  }

  @Test
  public void testTokenExchangeFlow() throws Exception {
    tokenHandler.accessToken = EXCHANGE_ACCESS_TOKEN;
    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(), GrantType.TOKEN_EXCHANGE.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN.getKey(), "subject-token");
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.getKey(),
        TokenTypeURI.ACCESS_TOKEN.toString());
    params.put(FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN.getKey(), "actor-token");
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.getKey(),
        TokenTypeURI.JWT.toString());
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE.getKey(),
        TokenTypeURI.ACCESS_TOKEN.toString());
    params.put(FlightSqlConnectionProperties.OAUTH_EXCHANGE_AUD.getKey(), "flight-service");
    params.put(
        FlightSqlConnectionProperties.OAUTH_RESOURCE.getKey(), "https://resource.example.com");
    params.put(FlightSqlConnectionProperties.OAUTH_SCOPE.getKey(), "profile email");
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey(), CLIENT_ID);
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey(), CLIENT_SECRET);

    connect();
    requestServerMetadata();

    CallHeaders headers = headerValidatorFactory.getHeadersReceivedAtRequest(0);
    assertEquals("Bearer " + EXCHANGE_ACCESS_TOKEN, headers.get("authorization"));

    assertEquals(1, tokenHandler.requestBodies.size());
    assertEquals(
        "urn:ietf:params:oauth:grant-type:token-exchange", tokenHandler.formValue(0, "grant_type"));
    assertEquals("subject-token", tokenHandler.formValue(0, "subject_token"));
    assertEquals(
        TokenTypeURI.ACCESS_TOKEN.toString(), tokenHandler.formValue(0, "subject_token_type"));
    assertEquals("actor-token", tokenHandler.formValue(0, "actor_token"));
    assertEquals(TokenTypeURI.JWT.toString(), tokenHandler.formValue(0, "actor_token_type"));
    assertEquals(
        TokenTypeURI.ACCESS_TOKEN.toString(), tokenHandler.formValue(0, "requested_token_type"));
    assertEquals("flight-service", tokenHandler.formValue(0, "audience"));
    assertEquals("https://resource.example.com", tokenHandler.formValue(0, "resource"));
    assertEquals("profile email", tokenHandler.formValue(0, "scope"));
    assertTrue(tokenHandler.authorizationHeaders.get(0).startsWith("Basic "));
  }

  @Test
  public void testClientCredentialsFlowWithHttpsTokenEndpointWithoutTrustStore() throws Exception {
    tokenHandler.accessToken = CLIENT_ACCESS_TOKEN;
    startHttpsTokenServer();
    clearJvmTrustStoreConfiguration();

    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(),
        GrantType.CLIENT_CREDENTIALS.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey(), CLIENT_ID);
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey(), CLIENT_SECRET);

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.IO, adbcException.getStatus());
  }

  @Test
  public void testClientCredentialsFlowWithHttpsTokenEndpointUsesJvmTrustStore() throws Exception {
    tokenHandler.accessToken = CLIENT_ACCESS_TOKEN;
    startHttpsTokenServer();
    configureJvmTrustStore(createTrustStorePath());

    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(),
        GrantType.CLIENT_CREDENTIALS.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey(), CLIENT_ID);
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey(), CLIENT_SECRET);

    connect();
    requestServerMetadata();

    CallHeaders headers = headerValidatorFactory.getHeadersReceivedAtRequest(0);
    assertEquals("Bearer " + CLIENT_ACCESS_TOKEN, headers.get("authorization"));
    assertEquals(1, tokenHandler.requestBodies.size());
    assertEquals("client_credentials", tokenHandler.formValue(0, "grant_type"));
  }

  @Test
  public void testAuthorizationHeaderConflictsWithOauth() {
    params.put(
        FlightSqlConnectionProperties.AUTHORIZATION_HEADER.getKey(), "Bearer existing-token");
    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(),
        GrantType.CLIENT_CREDENTIALS.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey(), CLIENT_ID);
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_SECRET.getKey(), CLIENT_SECRET);

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.INVALID_ARGUMENT, adbcException.getStatus());
  }

  @Test
  public void testMissingRequiredParamsClientCredentials() {
    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(),
        GrantType.CLIENT_CREDENTIALS.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(FlightSqlConnectionProperties.OAUTH_CLIENT_ID.getKey(), CLIENT_ID);

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.INVALID_ARGUMENT, adbcException.getStatus());
  }

  @Test
  public void testMissingRequiredParamsTokenExchange() {
    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(), GrantType.TOKEN_EXCHANGE.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN.getKey(), "subject-token");

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.INVALID_ARGUMENT, adbcException.getStatus());
  }

  @Test
  public void testActorTokenRequiresActorTokenType() {
    params.put(
        FlightSqlConnectionProperties.OAUTH_FLOW.getKey(), GrantType.TOKEN_EXCHANGE.getValue());
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN.getKey(), "subject-token");
    params.put(
        FlightSqlConnectionProperties.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.getKey(),
        TokenTypeURI.ACCESS_TOKEN.toString());
    params.put(FlightSqlConnectionProperties.OAUTH_EXCHANGE_ACTOR_TOKEN.getKey(), "actor-token");

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.INVALID_ARGUMENT, adbcException.getStatus());
  }

  @Test
  public void testInvalidOauthFlow() {
    params.put(FlightSqlConnectionProperties.OAUTH_FLOW.getKey(), "invalid-flow");
    params.put(FlightSqlConnectionProperties.OAUTH_TOKEN_URI.getKey(), tokenUri());

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, adbcException.getStatus());
  }

  private void connect() throws Exception {
    database =
        AdbcDriverManager.getInstance()
            .connect(FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
    connection = database.connect();
  }

  private void requestServerMetadata() throws Exception {
    try (ArrowReader reader = connection.getInfo(new int[] {AdbcInfoCode.VENDOR_NAME.getValue()})) {
      while (reader.loadNextBatch()) {
        // Only interested in triggering an authenticated RPC.
      }
    } catch (Exception ex) {
      // MockFlightSqlProducer does not implement the full SQL metadata surface.
    }
  }

  private String tokenUri() {
    return String.format(
        "%s://localhost:%d/token", tokenServerScheme, tokenServer.getAddress().getPort());
  }

  private void startHttpTokenServer() throws IOException {
    if (tokenServer != null) {
      tokenServer.stop(0);
    }
    tokenServerScheme = "http";
    tokenServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    tokenServer.createContext("/token", tokenHandler);
    tokenServer.start();
  }

  private void startHttpsTokenServer() throws Exception {
    if (tokenServer != null) {
      tokenServer.stop(0);
    }
    tokenServerScheme = "https";
    final SSLContext sslContext = createServerSslContext();
    final HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
    httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    httpsServer.createContext("/token", tokenHandler);
    httpsServer.start();
    tokenServer = httpsServer;
  }

  private void configureJvmTrustStore(Path trustStorePath) throws Exception {
    System.setProperty("javax.net.ssl.trustStore", trustStorePath.toString());
    System.setProperty("javax.net.ssl.trustStorePassword", new String(TRUST_STORE_PASSWORD));
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    refreshOAuthHttpsDefaults();
  }

  private void clearJvmTrustStoreConfiguration() throws Exception {
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");
    System.clearProperty("javax.net.ssl.trustStoreType");
    refreshOAuthHttpsDefaults();
  }

  private void restoreJvmTrustStoreConfiguration() {
    restoreSystemProperty("javax.net.ssl.trustStore", previousTrustStore);
    restoreSystemProperty("javax.net.ssl.trustStorePassword", previousTrustStorePassword);
    restoreSystemProperty("javax.net.ssl.trustStoreType", previousTrustStoreType);
    HTTPRequest.setDefaultSSLSocketFactory(previousDefaultSslSocketFactory);
    HTTPRequest.setDefaultHostnameVerifier(oauthHostnameVerifier());
  }

  private void refreshOAuthHttpsDefaults() throws Exception {
    final TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init((KeyStore) null);
    final SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());
    HTTPRequest.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HTTPRequest.setDefaultHostnameVerifier(oauthHostnameVerifier());
  }

  private Path createTrustStorePath() throws Exception {
    final KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("root", readCertificate(flightDataPath("root-ca.pem")));

    final Path trustStorePath = Files.createTempFile("oauth-truststore", ".p12");
    tempPaths.add(trustStorePath);
    try (OutputStream output = Files.newOutputStream(trustStorePath)) {
      trustStore.store(output, TRUST_STORE_PASSWORD);
    }
    return trustStorePath;
  }

  private SSLContext createServerSslContext() throws Exception {
    final Certificate certificate = readCertificate(flightDataPath("cert0.pem"));
    final PrivateKey privateKey = readPrivateKey(flightDataPath("cert0.pkcs1"));
    final KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "token-server", privateKey, TRUST_STORE_PASSWORD, new Certificate[] {certificate});

    final KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, TRUST_STORE_PASSWORD);

    final SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
    return sslContext;
  }

  private static X509Certificate readCertificate(Path path) throws Exception {
    try (InputStream input = Files.newInputStream(path)) {
      final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
      return (X509Certificate) certificateFactory.generateCertificate(input);
    }
  }

  private static PrivateKey readPrivateKey(Path path) throws Exception {
    final String pem = Files.readString(path, StandardCharsets.US_ASCII);
    final String base64 =
        pem.replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "")
            .replaceAll("\\s+", "");
    final byte[] der = Base64.getDecoder().decode(base64);
    return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
  }

  private static Path flightDataPath(String filename) {
    final String dataRoot = System.getProperty("arrow.test.dataRoot");
    if (dataRoot != null) {
      return Paths.get(dataRoot).resolve("flight").resolve(filename);
    }
    return Paths.get("testing", "data", "flight", filename).toAbsolutePath().normalize();
  }

  private static void restoreSystemProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private HostnameVerifier oauthHostnameVerifier() {
    if (previousDefaultHostnameVerifier != null) {
      return previousDefaultHostnameVerifier;
    }
    return HttpsURLConnection.getDefaultHostnameVerifier();
  }

  private static final class TokenHandler implements HttpHandler {
    private final List<String> requestBodies = new ArrayList<>();
    private final List<String> authorizationHeaders = new ArrayList<>();
    private String accessToken = CLIENT_ACCESS_TOKEN;

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      final String body =
          new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      requestBodies.add(body);
      authorizationHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));

      final byte[] responseBytes =
          ("{\"access_token\":\""
                  + accessToken
                  + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}")
              .getBytes(StandardCharsets.UTF_8);
      final Headers responseHeaders = exchange.getResponseHeaders();
      responseHeaders.add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, responseBytes.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(responseBytes);
      }
    }

    private String formValue(int requestIndex, String key) {
      return decodeForm(requestBodies.get(requestIndex)).get(key);
    }

    private static Map<String, String> decodeForm(String body) {
      final Map<String, String> values = new LinkedHashMap<>();
      if (body.isEmpty()) {
        return values;
      }
      for (String pair : body.split("&")) {
        final String[] parts = pair.split("=", 2);
        final String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
        final String value =
            parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
        values.put(name, value);
      }
      return values;
    }
  }
}
