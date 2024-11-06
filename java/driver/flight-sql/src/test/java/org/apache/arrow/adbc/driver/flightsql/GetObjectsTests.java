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

import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer.serializeSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.driver.jdbc.FlightServerTestExtension;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Test functions for returning database catalog information. */
public class GetObjectsTests {
  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();

  @RegisterExtension
  public static FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(FLIGHT_SQL_PRODUCER);

  private static final int BASE_ROW_COUNT = 10;
  private static AdbcConnection connection;

  private enum ExpectedColumnSelection {
    COLUMNS_DISABLED,
    EMPTY_COLUMNS,
    FIRST_COLUMN,
    ALL_COLUMNS
  }

  @BeforeAll
  public static void setUpBeforeClass() throws AdbcException {
    String uri =
        String.format("grpc+tcp://%s:%d", "localhost", FLIGHT_SERVER_TEST_EXTENSION.getPort());
    Map<String, Object> params = new HashMap<>();
    params.put(AdbcDriver.PARAM_URI.getKey(), uri);
    params.put(AdbcDriver.PARAM_USERNAME.getKey(), FlightServerTestExtension.DEFAULT_USER);
    params.put(AdbcDriver.PARAM_PASSWORD.getKey(), FlightServerTestExtension.DEFAULT_PASSWORD);
    params.put(FlightSqlConnectionProperties.WITH_COOKIE_MIDDLEWARE.getKey(), true);
    AdbcDatabase db =
        AdbcDriverManager.getInstance()
            .connect(FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
    connection = db.connect();

    final Message commandGetCatalogs = FlightSql.CommandGetCatalogs.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetCatalogsResultProducer =
        listener -> {
          try (final BufferAllocator allocator = new RootAllocator();
              final VectorSchemaRoot root =
                  VectorSchemaRoot.create(
                      FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, allocator)) {
            final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
            range(0, BASE_ROW_COUNT)
                .forEach(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))));

            // Add an extra catalog that is empty.
            catalogName.setSafe(BASE_ROW_COUNT, new Text("empty catalog"));

            // For testing catalogs with schemas with multiple tables.
            catalogName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema catalog"));

            // For testing catalogs with empty schemas.
            catalogName.setSafe(BASE_ROW_COUNT + 2, new Text("empty-schema catalog"));

            // For testing catalogs with schemas with multiple tables.
            catalogName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table catalog"));
            root.setRowCount(BASE_ROW_COUNT + 4);
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetCatalogs, commandGetCatalogsResultProducer);

    final Message commandGetDbSchemas = FlightSql.CommandGetDbSchemas.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetSchemasResultProducer =
        listener -> {
          try (final BufferAllocator allocator = new RootAllocator();
              final VectorSchemaRoot root =
                  VectorSchemaRoot.create(
                      FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
            final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
            final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
            range(0, BASE_ROW_COUNT)
                .peek(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))))
                .forEach(i -> schemaName.setSafe(i, new Text(format("schema #%d", i))));

            // Note: Intentionally do not show the empty catalog here.

            // Put multiple schemas in catalog "multi-schema".
            catalogName.setSafe(BASE_ROW_COUNT, new Text("multi-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT, new Text("multi-schema schema 0"));
            catalogName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema schema 1"));

            // For testing an empty schema.
            catalogName.setSafe(BASE_ROW_COUNT + 2, new Text("empty-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 2, new Text("empty-schema schema 0"));

            // For testing catalogs with schemas with multiple tables.
            catalogName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table schema"));
            root.setRowCount(BASE_ROW_COUNT + 4);
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetDbSchemas, commandGetSchemasResultProducer);

    final Message commandGetTables = FlightSql.CommandGetTables.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetTablesResultProducer =
        listener -> {
          try (final BufferAllocator allocator = new RootAllocator();
              final VectorSchemaRoot root =
                  VectorSchemaRoot.create(
                      FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
            final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
            final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
            final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
            final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
            range(0, BASE_ROW_COUNT)
                .peek(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))))
                .peek(i -> schemaName.setSafe(i, new Text(format("schema #%d", i))))
                .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
                .forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));

            // Explicitly don't add the empty catalog.

            // Put multiple schemas in catalog "multi-schema".
            catalogName.setSafe(BASE_ROW_COUNT, new Text("multi-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT, new Text("multi-schema schema 0"));
            tableName.setSafe(BASE_ROW_COUNT, new Text("multi-schema table 0"));
            tableType.setSafe(BASE_ROW_COUNT, new Text("multi-schema type 0"));
            catalogName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema schema 1"));
            tableName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema table 1"));
            tableType.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema type 1"));

            // Explicitly don't add the empty schema.

            // For testing catalogs with schemas with multiple tables.
            catalogName.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table schema"));
            tableName.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table table 0"));
            tableType.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table table type 0"));
            catalogName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table schema"));
            tableName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table table 1"));
            tableType.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table table type 1"));

            root.setRowCount(BASE_ROW_COUNT + 4);
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTables, commandGetTablesResultProducer);

    final Message commandGetTablesWithSchema =
        FlightSql.CommandGetTables.newBuilder().setIncludeSchema(true).build();
    final Consumer<FlightProducer.ServerStreamListener> commandGetTablesWithSchemaResultProducer =
        listener -> {
          try (final BufferAllocator allocator = new RootAllocator();
              final VectorSchemaRoot root =
                  VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA, allocator)) {
            final byte[] filledTableSchemaBytes =
                copyFrom(
                        serializeSchema(
                            new Schema(
                                Arrays.asList(
                                    Field.nullable(
                                        "column_1", ArrowType.Decimal.createDecimal(5, 2, 128)),
                                    Field.nullable(
                                        "column_2",
                                        new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                                    Field.notNullable("column_3", Types.MinorType.INT.getType())))))
                    .toByteArray();
            final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
            final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
            final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
            final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
            final VarBinaryVector tableSchema = (VarBinaryVector) root.getVector("table_schema");
            range(0, BASE_ROW_COUNT)
                .peek(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))))
                .peek(i -> schemaName.setSafe(i, new Text(format("schema #%d", i))))
                .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
                .peek(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))))
                .forEach(i -> tableSchema.setSafe(i, filledTableSchemaBytes));

            // Explicitly don't add the empty catalog.

            // Put multiple schemas in catalog "multi-schema".
            catalogName.setSafe(BASE_ROW_COUNT, new Text("multi-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT, new Text("multi-schema schema 0"));
            tableName.setSafe(BASE_ROW_COUNT, new Text("multi-schema table 0"));
            tableType.setSafe(BASE_ROW_COUNT, new Text("multi-schema type 0"));
            tableSchema.setSafe(BASE_ROW_COUNT, filledTableSchemaBytes);
            catalogName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema schema 1"));
            tableName.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema table 1"));
            tableType.setSafe(BASE_ROW_COUNT + 1, new Text("multi-schema type 1"));
            tableSchema.setSafe(BASE_ROW_COUNT + 1, filledTableSchemaBytes);

            // Explicitly don't add the empty schema.

            // For testing catalogs with schemas with multiple tables.
            catalogName.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table schema"));
            tableName.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table table 0"));
            tableType.setSafe(BASE_ROW_COUNT + 2, new Text("multi-table table type 0"));
            tableSchema.setSafe(BASE_ROW_COUNT + 2, filledTableSchemaBytes);
            catalogName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table catalog"));
            schemaName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table schema"));
            tableName.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table table 1"));
            tableType.setSafe(BASE_ROW_COUNT + 3, new Text("multi-table table type 1"));
            tableSchema.setSafe(BASE_ROW_COUNT + 3, filledTableSchemaBytes);

            root.setRowCount(BASE_ROW_COUNT + 4);
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(
        commandGetTablesWithSchema, commandGetTablesWithSchemaResultProducer);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    AutoCloseables.close(connection, FLIGHT_SQL_PRODUCER, allocator);
  }

  @Test
  public void testGetCatalogs() throws IOException, AdbcException {
    try (final ArrowReader resultSet =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.CATALOGS, null, null, null, null, null)) {
      boolean loadedBatch = resultSet.loadNextBatch();
      assertTrue(loadedBatch);
      VectorSchemaRoot root = resultSet.getVectorSchemaRoot();
      VarCharVector catalogNameVector = (VarCharVector) root.getVector(0);
      FieldVector schemas = root.getVector(1);
      for (int i = 0; i < BASE_ROW_COUNT; ++i) {
        assertEquals(String.format("catalog #%d", i), catalogNameVector.getObject(i).toString());
        assertTrue(schemas.isNull(i));
      }
      assertEquals(BASE_ROW_COUNT + 4, root.getRowCount());
    }
  }

  @Test
  public void testGetCatalogsWithCatalogFilter() throws IOException, AdbcException {
    try (final ArrowReader resultSet =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.CATALOGS, "catalog #0", null, null, null, null)) {
      boolean loadedBatch = resultSet.loadNextBatch();
      assertTrue(loadedBatch);
      VectorSchemaRoot root = resultSet.getVectorSchemaRoot();
      VarCharVector catalogNameVector = (VarCharVector) root.getVector(0);
      FieldVector schemas = root.getVector(1);
      assertEquals(1, root.getRowCount());
      assertEquals(String.format("catalog #%d", 0), catalogNameVector.getObject(0).toString());
      assertTrue(schemas.isNull(0));
    }
  }

  @Test
  public void testGetCatalogsWithEmptyingCatalogFilter() throws IOException, AdbcException {
    try (final ArrowReader resultSet =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.CATALOGS, "invalid filter", null, null, null, null)) {
      boolean loadedBatch = resultSet.loadNextBatch();
      assertFalse(loadedBatch);
    }
  }

  @Test
  public void testGetSchemas() throws IOException, AdbcException {
    try (final ArrowReader resultSet =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
      boolean loadedBatch = resultSet.loadNextBatch();
      assertTrue(loadedBatch);
      VectorSchemaRoot root = resultSet.getVectorSchemaRoot();
      VarCharVector catalogNameVector = (VarCharVector) root.getVector(0);
      // The schema is a List of structs.
      ListVector schemaVector = (ListVector) root.getVector(1);
      for (int i = 0; i < BASE_ROW_COUNT; ++i) {
        assertEquals(String.format("catalog #%d", i), catalogNameVector.getObject(i).toString());
        Map<String, Object> schemaStruct = (Map<String, Object>) schemaVector.getObject(i).get(0);
        String entry = schemaStruct.get("db_schema_name").toString();
        assertEquals(String.format("schema #%d", i), entry);
        assertNull(schemaStruct.get("db_schema_tables"));
      }

      // Ensure empty catalogs are shown correctly.
      assertEquals("empty catalog", catalogNameVector.getObject(BASE_ROW_COUNT).toString());
      assertTrue(schemaVector.getObject(BASE_ROW_COUNT).isEmpty());

      // Ensure multi-schema catalogs are shown correctly.
      assertEquals(
          "multi-schema catalog", catalogNameVector.getObject(BASE_ROW_COUNT + 1).toString());
      Map<String, Object> schemaStruct =
          (Map<String, Object>) schemaVector.getObject(BASE_ROW_COUNT + 1).get(0);
      String entry = schemaStruct.get("db_schema_name").toString();
      assertEquals("multi-schema schema 0", entry);
      assertNull(schemaStruct.get("db_schema_tables"));
      schemaStruct = (Map<String, Object>) schemaVector.getObject(BASE_ROW_COUNT + 1).get(1);
      entry = schemaStruct.get("db_schema_name").toString();
      assertEquals("multi-schema schema 1", entry);
      assertNull(schemaStruct.get("db_schema_tables"));

      assertEquals(BASE_ROW_COUNT + 4, root.getRowCount());
    }
  }

  @Test
  public void testGetTables() throws IOException, AdbcException {
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)) {
      validateTables(reader, ExpectedColumnSelection.COLUMNS_DISABLED);
    }
  }

  @Test
  public void testGetColumns() throws IOException, AdbcException {
    try (final ArrowReader reader =
        connection.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, null)) {
      validateTables(reader, ExpectedColumnSelection.ALL_COLUMNS);
    }
  }

  @Test
  public void testGetColumnsWithColumnFilter() throws IOException, AdbcException {
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, "column_1")) {
      validateTables(reader, ExpectedColumnSelection.FIRST_COLUMN);
    }
  }

  @Test
  public void testGetColumnsWithEmptyingColumnFilter() throws IOException, AdbcException {
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, "empty filter")) {
      validateTables(reader, ExpectedColumnSelection.EMPTY_COLUMNS);
    }
  }

  private void validateTables(ArrowReader reader, ExpectedColumnSelection columnSelection)
      throws AdbcException, IOException {
    boolean loadedBatch = reader.loadNextBatch();
    assertTrue(loadedBatch);
    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    VarCharVector catalogNameVector = (VarCharVector) root.getVector(0);
    // The schema is a List of structs.
    ListVector schemaVector = (ListVector) root.getVector(1);
    for (int i = 0; i < BASE_ROW_COUNT; ++i) {
      assertEquals(String.format("catalog #%d", i), catalogNameVector.getObject(i).toString());
      Map<String, Object> schemaStruct = (Map<String, Object>) schemaVector.getObject(i).get(0);
      String entry = schemaStruct.get("db_schema_name").toString();
      assertEquals(String.format("schema #%d", i), entry);
      List<Map<String, Object>> tableStructs =
          (List<Map<String, Object>>) schemaStruct.get("db_schema_tables");
      Map<String, Object> tableStruct = tableStructs.get(0);
      assertNotNull(tableStruct);
      assertEquals(String.format("table_name #%d", i), tableStruct.get("table_name").toString());
      assertEquals(String.format("table_type #%d", i), tableStruct.get("table_type").toString());
      validateColumns(tableStruct, columnSelection);
    }

    // Ensure empty catalogs are shown correctly.
    assertEquals("empty catalog", catalogNameVector.getObject(BASE_ROW_COUNT).toString());
    assertTrue(schemaVector.getObject(BASE_ROW_COUNT).isEmpty());

    // Ensure multi-schema catalogs are shown correctly.
    assertEquals(
        "multi-schema catalog", catalogNameVector.getObject(BASE_ROW_COUNT + 1).toString());
    Map<String, Object> schemaStruct =
        (Map<String, Object>) schemaVector.getObject(BASE_ROW_COUNT + 1).get(0);
    String entry = schemaStruct.get("db_schema_name").toString();
    assertEquals("multi-schema schema 0", entry);
    List<Map<String, Object>> tableStructs =
        (List<Map<String, Object>>) schemaStruct.get("db_schema_tables");
    Map<String, Object> tableStruct = tableStructs.get(0);
    assertNotNull(tableStruct);
    assertEquals("multi-schema table 0", tableStruct.get("table_name").toString());
    assertEquals("multi-schema type 0", tableStruct.get("table_type").toString());
    validateColumns(tableStruct, columnSelection);
    schemaStruct = (Map<String, Object>) schemaVector.getObject(BASE_ROW_COUNT + 1).get(1);
    entry = schemaStruct.get("db_schema_name").toString();
    assertEquals("multi-schema schema 1", entry);
    entry = schemaStruct.get("db_schema_name").toString();
    assertEquals("multi-schema schema 1", entry);
    tableStructs = (List<Map<String, Object>>) schemaStruct.get("db_schema_tables");
    tableStruct = tableStructs.get(0);
    assertNotNull(tableStruct);
    assertEquals("multi-schema table 1", tableStruct.get("table_name").toString());
    assertEquals("multi-schema type 1", tableStruct.get("table_type").toString());
    validateColumns(tableStruct, columnSelection);

    // Ensure empty schemas are shown correctly.
    assertEquals(
        "empty-schema catalog", catalogNameVector.getObject(BASE_ROW_COUNT + 2).toString());
    schemaStruct = (Map<String, Object>) schemaVector.getObject(BASE_ROW_COUNT + 2).get(0);
    entry = schemaStruct.get("db_schema_name").toString();
    assertEquals("empty-schema schema 0", entry);
    tableStructs = (List<Map<String, Object>>) schemaStruct.get("db_schema_tables");
    assertTrue(tableStructs.isEmpty());

    // Ensure schemas with multiple tables appear correctly.
    assertEquals("multi-table catalog", catalogNameVector.getObject(BASE_ROW_COUNT + 3).toString());
    schemaStruct = (Map<String, Object>) schemaVector.getObject(BASE_ROW_COUNT + 3).get(0);
    entry = schemaStruct.get("db_schema_name").toString();
    assertEquals("multi-table schema", entry);
    tableStructs = (List<Map<String, Object>>) schemaStruct.get("db_schema_tables");
    tableStruct = tableStructs.get(0);
    assertEquals("multi-table table 0", tableStruct.get("table_name").toString());
    assertEquals("multi-table table type 0", tableStruct.get("table_type").toString());
    validateColumns(tableStruct, columnSelection);
    tableStruct = tableStructs.get(1);
    assertEquals("multi-table table 1", tableStruct.get("table_name").toString());
    assertEquals("multi-table table type 1", tableStruct.get("table_type").toString());
    validateColumns(tableStruct, columnSelection);
  }

  private void validateColumns(
      Map<String, Object> tableStruct, ExpectedColumnSelection columnSelection) {
    List<Map<String, Object>> columns =
        (List<Map<String, Object>>) tableStruct.get("table_columns");
    if (columnSelection == ExpectedColumnSelection.COLUMNS_DISABLED) {
      assertNull(columns);
      return;
    }

    if (columnSelection == ExpectedColumnSelection.EMPTY_COLUMNS) {
      assertTrue(columns.isEmpty());
      return;
    }

    assertFalse(columns.isEmpty());
    Map<String, Object> columnStruct = columns.get(0);
    assertEquals("column_1", columnStruct.get("column_name").toString());
    assertEquals("1", columnStruct.get("ordinal_position").toString());
    assertEquals("2", columnStruct.get("xdbc_decimal_digits").toString());
    assertEquals("YES", columnStruct.get("xdbc_is_nullable").toString());

    if (columnSelection == ExpectedColumnSelection.ALL_COLUMNS) {
      columnStruct = columns.get(1);
      assertEquals("column_2", columnStruct.get("column_name").toString());
      assertEquals("2", columnStruct.get("ordinal_position").toString());
      assertNull(columnStruct.get("xdbc_num_prec_radix"));
      assertEquals("YES", columnStruct.get("xdbc_is_nullable").toString());

      columnStruct = columns.get(2);
      assertEquals("column_3", columnStruct.get("column_name").toString());
      assertEquals("3", columnStruct.get("ordinal_position").toString());
      assertEquals((short) 0, columnStruct.get("xdbc_decimal_digits"));
      assertEquals("NO", columnStruct.get("xdbc_is_nullable").toString());
    }
  }
}
