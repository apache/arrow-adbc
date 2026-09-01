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
package org.apache.arrow.adbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BulkIngestBuilderTest {
  Schema schema = new Schema(List.of(Field.nullable("foo", Types.MinorType.INT.getType())));
  BufferAllocator allocator;
  TestConnection connection;
  VectorSchemaRoot root;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
    connection = new TestConnection();
    root = VectorSchemaRoot.create(schema, allocator);
  }

  @AfterEach
  void afterEach() {
    root.close();
    connection.close();
    allocator.close();
  }

  @Test
  void noTable() throws Exception {
    try (var ingest = connection.bulkIngest()) {
      assertThatThrownBy(ingest::ingest)
          .hasMessageContaining("must set targetTable")
          .isInstanceOf(AdbcException.class);
    }
  }

  @Test
  void noRoot() throws Exception {
    try (var ingest = connection.bulkIngest()) {
      ingest.targetTable("foobar");
      assertThatThrownBy(ingest::ingest)
          .hasMessageContaining("must bind")
          .isInstanceOf(AdbcException.class);
    }
  }

  @Test
  void standardOptions() throws Exception {
    try (var ingest = connection.bulkIngest()) {
      ingest
          .targetTable("foobar")
          .bind(root)
          .temporary(true)
          .targetCatalog("catalog")
          .targetSchema("schema")
          .createAppendMode()
          .ingest();
    }
    assertThat(connection.targetTable).isEqualTo("foobar");
    assertThat(connection.mode).isEqualTo(BulkIngestMode.CREATE_APPEND);
    assertThat(connection.options)
        .anyMatch(
            option ->
                (option instanceof IngestOption.TargetNamespaceIngestOption)
                    && Objects.equals(
                        ((IngestOption.TargetNamespaceIngestOption) option).getTargetDbSchema(),
                        "schema"));
    assertThat(connection.options)
        .anyMatch(
            option ->
                (option instanceof IngestOption.TargetNamespaceIngestOption)
                    && Objects.equals(
                        ((IngestOption.TargetNamespaceIngestOption) option).getTargetCatalog(),
                        "catalog"));
    assertThat(connection.options)
        .anyMatch(
            option ->
                (option instanceof IngestOption.TemporaryIngestOption)
                    && ((IngestOption.TemporaryIngestOption) option).isTemporary());
    assertThat(connection.customOptions).isEmpty();
  }

  @Test
  void customOptions() throws Exception {
    var key = new TypedKey<>("foobar", Integer.class);
    try (var ingest = connection.bulkIngest()) {
      ingest
          .targetTable("foobar")
          .bind(root)
          .option(new CustomIngestOption())
          .option(key, 64)
          .ingest();
    }
    assertThat(connection.targetTable).isEqualTo("foobar");
    assertThat(connection.mode).isEqualTo(BulkIngestMode.CREATE);

    assertThat(connection.options).anyMatch(option -> option instanceof CustomIngestOption);
    assertThat(connection.customOptions).containsExactly(Map.entry(key, 64));
  }

  @Test
  void closesReader() throws Exception {
    assertThatThrownBy(
            () -> {
              try (var ingest = connection.bulkIngest()) {
                ingest.bind(new TestArrowReader(allocator));
              }
            })
        .isInstanceOf(AdbcException.class)
        .hasMessageContaining("failed to close ArrowReader")
        .hasCauseInstanceOf(IOException.class)
        .cause()
        .hasMessageContaining("expected");

    assertThatThrownBy(
            () -> {
              try (var ingest = connection.bulkIngest()) {
                ingest.bind(new TestArrowReader(allocator));
                ingest.bind(root);
              }
            })
        .isInstanceOf(AdbcException.class)
        .hasMessageContaining("failed to close existing ArrowReader")
        .hasCauseInstanceOf(IOException.class)
        .cause()
        .hasMessageContaining("expected");
  }

  @Test
  void noCloseRoot() throws Exception {
    try (var ingest = connection.bulkIngest()) {
      ingest.bind(root);
    }
    root.setRowCount(5);
    assertThat(root.contentToTSVString()).isNotEmpty();
  }

  @Test
  void exclusiveRootReader() throws Exception {
    try (var ingest = connection.bulkIngest()) {
      ingest.targetTable("foobar");
      ingest.bind(root);
      ingest.bind(new NoOpArrowReader(allocator));
      ingest.ingest();
    }
    assertThat(connection.reader).isNotNull();
    assertThat(connection.root).isNull();
  }

  @Test
  void exclusiveReaderRoot() throws Exception {
    try (var ingest = connection.bulkIngest()) {
      ingest.targetTable("foobar");
      ingest.bind(new NoOpArrowReader(allocator));
      ingest.bind(root);
      ingest.ingest();
    }
    assertThat(connection.reader).isNull();
    assertThat(connection.root).isNotNull();
  }

  static class TestConnection implements AdbcConnection {
    String targetTable;
    BulkIngestMode mode;
    IngestOption[] options;
    List<Map.Entry<TypedKey<?>, Object>> customOptions = new ArrayList<>();
    VectorSchemaRoot root;
    ArrowReader reader;

    @Override
    public AdbcStatement bulkIngest(
        String targetTableName, BulkIngestMode mode, IngestOption... options) {
      var stmt = new TestStatement(this);
      this.targetTable = targetTableName;
      this.mode = mode;
      this.options = options;
      return stmt;
    }

    @Override
    public AdbcStatement createStatement() throws AdbcException {
      return new TestStatement(this);
    }

    @Override
    public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  static class TestStatement implements AdbcStatement {
    private final TestConnection connection;

    public TestStatement(TestConnection testConnection) {
      this.connection = testConnection;
    }

    @Override
    public void bind(ArrowReader reader) throws AdbcException {
      connection.reader = reader;
    }

    @Override
    public void bind(VectorSchemaRoot root) throws AdbcException {
      connection.root = root;
    }

    @Override
    public QueryResult executeQuery() {
      throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult executeUpdate() {
      return new UpdateResult(-1);
    }

    @Override
    public void prepare() {}

    @Override
    public void close() {}

    @Override
    public <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
      connection.customOptions.add(Map.entry(key, value));
    }
  }

  static class CustomIngestOption implements IngestOption {}

  static class TestArrowReader extends ArrowReader {
    TestArrowReader(BufferAllocator allocator) {
      super(allocator);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      return false;
    }

    @Override
    public long bytesRead() {
      return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {}

    @Override
    protected Schema readSchema() throws IOException {
      return null;
    }

    @Override
    public void close() throws IOException {
      throw new IOException("expected");
    }
  }

  static class NoOpArrowReader extends ArrowReader {
    NoOpArrowReader(BufferAllocator allocator) {
      super(allocator);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      return false;
    }

    @Override
    public long bytesRead() {
      return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {}

    @Override
    protected Schema readSchema() throws IOException {
      return null;
    }
  }
}
