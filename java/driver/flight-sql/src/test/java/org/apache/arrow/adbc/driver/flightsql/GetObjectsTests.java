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
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer.serializeSchema;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert.SQL_CONVERT_BIGINT_VALUE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert.SQL_CONVERT_BIT_VALUE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert.SQL_CONVERT_INTEGER_VALUE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.driver.jdbc.FlightServerTestRule;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.ResultSetTestUtils;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

/**
 * Test functions for returning database catalog information.
 */
public class GetObjectsTests {

  public static final boolean EXPECTED_MAX_ROW_SIZE_INCLUDES_BLOBS = false;
  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE = FlightServerTestRule
      .createStandardTestRule(FLIGHT_SQL_PRODUCER);
  private static final int ROW_COUNT = 10;
  private static final List<List<Object>> EXPECTED_GET_CATALOGS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> format("catalog #%d", i))
          .map(Object.class::cast)
          .map(Collections::singletonList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_TABLE_TYPES_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> format("table_type #%d", i))
          .map(Object.class::cast)
          .map(Collections::singletonList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_TABLES_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("db_schema_name #%d", i),
              format("table_name #%d", i),
              format("table_type #%d", i),
              // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
              null, null, null, null, null, null})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_SCHEMAS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("db_schema_name #%d", i),
              format("catalog_name #%d", i)})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("pk_catalog_name #%d", i),
              format("pk_db_schema_name #%d", i),
              format("pk_table_name #%d", i),
              format("pk_column_name #%d", i),
              format("fk_catalog_name #%d", i),
              format("fk_db_schema_name #%d", i),
              format("fk_table_name #%d", i),
              format("fk_column_name #%d", i),
              i,
              format("fk_key_name #%d", i),
              format("pk_key_name #%d", i),
              (byte) i,
              (byte) i,
              // TODO Add this field to FlightSQL, as it's currently not possible to fetch it.
              null})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_CROSS_REFERENCE_RESULTS =
      EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS;
  private static final List<List<Object>> EXPECTED_PRIMARY_KEYS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("db_schema_name #%d", i),
              format("table_name #%d", i),
              format("column_name #%d", i),
              i,
              format("key_name #%d", i)})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<String> FIELDS_GET_IMPORTED_EXPORTED_KEYS = ImmutableList.of(
      "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
      "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
      "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
      "FK_NAME", "PK_NAME", "UPDATE_RULE", "DELETE_RULE",
      "DEFERRABILITY");
  private static final List<String> FIELDS_GET_CROSS_REFERENCE = FIELDS_GET_IMPORTED_EXPORTED_KEYS;
  private static final String TARGET_TABLE = "TARGET_TABLE";
  private static final String TARGET_FOREIGN_TABLE = "FOREIGN_TABLE";
  private static final String EXPECTED_DATABASE_PRODUCT_NAME = "Test Server Name";
  private static final String EXPECTED_DATABASE_PRODUCT_VERSION = "v0.0.1-alpha";
  private static final String EXPECTED_IDENTIFIER_QUOTE_STRING = "\"";
  private static final boolean EXPECTED_IS_READ_ONLY = true;
  private static final String EXPECTED_SQL_KEYWORDS =
      "ADD, ADD CONSTRAINT, ALTER, ALTER TABLE, ANY, USER, TABLE";
  private static final String EXPECTED_NUMERIC_FUNCTIONS =
      "ABS(), ACOS(), ASIN(), ATAN(), CEIL(), CEILING(), COT()";
  private static final String EXPECTED_STRING_FUNCTIONS =
      "ASCII, CHAR, CHARINDEX, CONCAT, CONCAT_WS, FORMAT, LEFT";
  private static final String EXPECTED_SYSTEM_FUNCTIONS =
      "CAST, CONVERT, CHOOSE, ISNULL, IS_NUMERIC, IIF, TRY_CAST";
  private static final String EXPECTED_TIME_DATE_FUNCTIONS =
      "GETDATE(), DATEPART(), DATEADD(), DATEDIFF()";
  private static final String EXPECTED_SEARCH_STRING_ESCAPE = "\\";
  private static final String EXPECTED_EXTRA_NAME_CHARACTERS = "";
  private static final boolean EXPECTED_SUPPORTS_COLUMN_ALIASING = true;
  private static final boolean EXPECTED_NULL_PLUS_NULL_IS_NULL = true;
  private static final boolean EXPECTED_SUPPORTS_TABLE_CORRELATION_NAMES = true;
  private static final boolean EXPECTED_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES = false;
  private static final boolean EXPECTED_EXPRESSIONS_IN_ORDER_BY = true;
  private static final boolean EXPECTED_SUPPORTS_ORDER_BY_UNRELATED = true;
  private static final boolean EXPECTED_SUPPORTS_LIKE_ESCAPE_CLAUSE = true;
  private static final boolean EXPECTED_NON_NULLABLE_COLUMNS = true;
  private static final String EXPECTED_SCHEMA_TERM = "schema";
  private static final String EXPECTED_PROCEDURE_TERM = "procedure";
  private static final String EXPECTED_CATALOG_TERM = "catalog";
  private static final boolean EXPECTED_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY = true;
  private static final boolean EXPECTED_CATALOG_AT_START = true;
  private static final boolean EXPECTED_SELECT_FOR_UPDATE_SUPPORTED = false;
  private static final boolean EXPECTED_STORED_PROCEDURES_SUPPORTED = false;
  private static final FlightSql.SqlSupportedSubqueries[] EXPECTED_SUPPORTED_SUBQUERIES = new FlightSql.SqlSupportedSubqueries[]
      {FlightSql.SqlSupportedSubqueries.SQL_SUBQUERIES_IN_COMPARISONS};
  private static final boolean EXPECTED_CORRELATED_SUBQUERIES_SUPPORTED = true;
  private static final int EXPECTED_MAX_BINARY_LITERAL_LENGTH = 0;
  private static final int EXPECTED_MAX_CHAR_LITERAL_LENGTH = 0;
  private static final int EXPECTED_MAX_COLUMN_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_COLUMNS_IN_GROUP_BY = 0;
  private static final int EXPECTED_MAX_COLUMNS_IN_INDEX = 0;
  private static final int EXPECTED_MAX_COLUMNS_IN_ORDER_BY = 0;
  private static final int EXPECTED_MAX_COLUMNS_IN_SELECT = 0;
  private static final int EXPECTED_MAX_CONNECTIONS = 0;
  private static final int EXPECTED_MAX_CURSOR_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_INDEX_LENGTH = 0;
  private static final int EXPECTED_SCHEMA_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_PROCEDURE_NAME_LENGTH = 0;
  private static final int EXPECTED_MAX_CATALOG_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_ROW_SIZE = 0;
  private static final int EXPECTED_MAX_STATEMENT_LENGTH = 0;
  private static final int EXPECTED_MAX_STATEMENTS = 0;
  private static final int EXPECTED_MAX_TABLE_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_TABLES_IN_SELECT = 0;
  private static final int EXPECTED_MAX_USERNAME_LENGTH = 1024;
  private static final int EXPECTED_DEFAULT_TRANSACTION_ISOLATION = 0;
  private static final boolean EXPECTED_TRANSACTIONS_SUPPORTED = false;
  private static final boolean EXPECTED_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT = true;
  private static final boolean EXPECTED_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED = false;
  private static final boolean EXPECTED_BATCH_UPDATES_SUPPORTED = true;
  private static final boolean EXPECTED_SAVEPOINTS_SUPPORTED = false;
  private static final boolean EXPECTED_NAMED_PARAMETERS_SUPPORTED = false;
  private static final boolean EXPECTED_LOCATORS_UPDATE_COPY = true;
  private static final boolean EXPECTED_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED = false;
  private static final List<List<Object>> EXPECTED_GET_COLUMNS_RESULTS;
  private static AdbcConnection connection;

  static {
    List<Integer> expectedGetColumnsDataTypes = Arrays.asList(3, 93, 4);
    List<String> expectedGetColumnsTypeName = Arrays.asList("DECIMAL", "TIMESTAMP", "INTEGER");
    List<Integer> expectedGetColumnsRadix = Arrays.asList(10, null, 10);
    List<Integer> expectedGetColumnsColumnSize = Arrays.asList(5, 29, 10);
    List<Integer> expectedGetColumnsDecimalDigits = Arrays.asList(2, 9, 0);
    List<String> expectedGetColumnsIsNullable = Arrays.asList("YES", "YES", "NO");
    EXPECTED_GET_COLUMNS_RESULTS = range(0, ROW_COUNT * 3)
        .mapToObj(i -> new Object[] {
            format("catalog_name #%d", i / 3),
            format("db_schema_name #%d", i / 3),
            format("table_name%d", i / 3),
            format("column_%d", (i % 3) + 1),
            expectedGetColumnsDataTypes.get(i % 3),
            expectedGetColumnsTypeName.get(i % 3),
            expectedGetColumnsColumnSize.get(i % 3),
            null,
            expectedGetColumnsDecimalDigits.get(i % 3),
            expectedGetColumnsRadix.get(i % 3),
            !Objects.equals(expectedGetColumnsIsNullable.get(i % 3), "NO") ? 1 : 0,
            null, null, null, null, null,
            (i % 3) + 1,
            expectedGetColumnsIsNullable.get(i % 3),
            null, null, null, null,
            "", ""})
        .map(Arrays::asList)
        .collect(toList());
  }

  @Rule
  public final ErrorCollector collector = new ErrorCollector();
  public final ResultSetTestUtils resultSetTestUtils = new ResultSetTestUtils(collector);

  @BeforeClass
  public static void setUpBeforeClass() throws AdbcException {
    String uri = String.format("grpc+tcp://%s:%d", "localhost", FLIGHT_SERVER_TEST_RULE.getPort());
    Map<String, Object> params = new HashMap<>();
    params.put(AdbcDriver.PARAM_URI.getKey(), uri);
    params.put(AdbcDriver.PARAM_USERNAME.getKey(), FlightServerTestRule.DEFAULT_USER);
    params.put(AdbcDriver.PARAM_PASSWORD.getKey(), FlightServerTestRule.DEFAULT_PASSWORD);
    params.put(FlightSqlConnectionProperties.WITH_COOKIE_MIDDLEWARE.getKey(), true);
    AdbcDatabase db =
        AdbcDriverManager.getInstance()
            .connect(FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
    connection = db.connect();

    final Message commandGetCatalogs = FlightSql.CommandGetCatalogs.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetCatalogsResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA,
               allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        range(0, ROW_COUNT).forEach(
            i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetCatalogs, commandGetCatalogsResultProducer);

    final Message commandGetTableTypes = FlightSql.CommandGetTableTypes.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetTableTypesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA,
               allocator)) {
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT).forEach(
            i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTableTypes, commandGetTableTypesResultProducer);

    final Message commandGetTables = FlightSql.CommandGetTables.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetTablesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(
               FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("db_schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTables, commandGetTablesResultProducer);

    final Message commandGetTablesWithSchema = FlightSql.CommandGetTables.newBuilder()
        .setIncludeSchema(true)
        .build();
    final Consumer<FlightProducer.ServerStreamListener> commandGetTablesWithSchemaResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA,
               allocator)) {
        final byte[] filledTableSchemaBytes =
            copyFrom(
                serializeSchema(new Schema(Arrays.asList(
                    Field.nullable("column_1", ArrowType.Decimal.createDecimal(5, 2, 128)),
                    Field.nullable("column_2", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                    Field.notNullable("column_3", Types.MinorType.INT.getType())))))
                .toByteArray();
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        final VarBinaryVector tableSchema = (VarBinaryVector) root.getVector("table_schema");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("db_schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name%d", i))))
            .peek(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))))
            .forEach(i -> tableSchema.setSafe(i, filledTableSchemaBytes));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTablesWithSchema,
        commandGetTablesWithSchemaResultProducer);

    final Message commandGetDbSchemas = FlightSql.CommandGetDbSchemas.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetSchemasResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA,
               allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .forEach(i -> schemaName.setSafe(i, new Text(format("db_schema_name #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetDbSchemas, commandGetSchemasResultProducer);

    final Message commandGetExportedKeys =
        FlightSql.CommandGetExportedKeys.newBuilder().setTable(TARGET_TABLE).build();
    final Message commandGetImportedKeys =
        FlightSql.CommandGetImportedKeys.newBuilder().setTable(TARGET_TABLE).build();
    final Message commandGetCrossReference = FlightSql.CommandGetCrossReference.newBuilder()
        .setPkTable(TARGET_TABLE)
        .setFkTable(TARGET_FOREIGN_TABLE)
        .build();
    final Consumer<FlightProducer.ServerStreamListener> commandGetExportedAndImportedKeysResultProducer =
        listener -> {
          try (final BufferAllocator allocator = new RootAllocator();
               final VectorSchemaRoot root = VectorSchemaRoot.create(
                   FlightSqlProducer.Schemas.GET_IMPORTED_KEYS_SCHEMA,
                   allocator)) {
            final VarCharVector pkCatalogName = (VarCharVector) root.getVector("pk_catalog_name");
            final VarCharVector pkSchemaName = (VarCharVector) root.getVector("pk_db_schema_name");
            final VarCharVector pkTableName = (VarCharVector) root.getVector("pk_table_name");
            final VarCharVector pkColumnName = (VarCharVector) root.getVector("pk_column_name");
            final VarCharVector fkCatalogName = (VarCharVector) root.getVector("fk_catalog_name");
            final VarCharVector fkSchemaName = (VarCharVector) root.getVector("fk_db_schema_name");
            final VarCharVector fkTableName = (VarCharVector) root.getVector("fk_table_name");
            final VarCharVector fkColumnName = (VarCharVector) root.getVector("fk_column_name");
            final IntVector keySequence = (IntVector) root.getVector("key_sequence");
            final VarCharVector fkKeyName = (VarCharVector) root.getVector("fk_key_name");
            final VarCharVector pkKeyName = (VarCharVector) root.getVector("pk_key_name");
            final UInt1Vector updateRule = (UInt1Vector) root.getVector("update_rule");
            final UInt1Vector deleteRule = (UInt1Vector) root.getVector("delete_rule");
            range(0, ROW_COUNT)
                .peek(i -> pkCatalogName.setSafe(i, new Text(format("pk_catalog_name #%d", i))))
                .peek(i -> pkSchemaName.setSafe(i, new Text(format("pk_db_schema_name #%d", i))))
                .peek(i -> pkTableName.setSafe(i, new Text(format("pk_table_name #%d", i))))
                .peek(i -> pkColumnName.setSafe(i, new Text(format("pk_column_name #%d", i))))
                .peek(i -> fkCatalogName.setSafe(i, new Text(format("fk_catalog_name #%d", i))))
                .peek(i -> fkSchemaName.setSafe(i, new Text(format("fk_db_schema_name #%d", i))))
                .peek(i -> fkTableName.setSafe(i, new Text(format("fk_table_name #%d", i))))
                .peek(i -> fkColumnName.setSafe(i, new Text(format("fk_column_name #%d", i))))
                .peek(i -> keySequence.setSafe(i, i))
                .peek(i -> fkKeyName.setSafe(i, new Text(format("fk_key_name #%d", i))))
                .peek(i -> pkKeyName.setSafe(i, new Text(format("pk_key_name #%d", i))))
                .peek(i -> updateRule.setSafe(i, i))
                .forEach(i -> deleteRule.setSafe(i, i));
            root.setRowCount(ROW_COUNT);
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetExportedKeys,
        commandGetExportedAndImportedKeysResultProducer);
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetImportedKeys,
        commandGetExportedAndImportedKeysResultProducer);
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetCrossReference,
        commandGetExportedAndImportedKeysResultProducer);

    final Message commandGetPrimaryKeys =
        FlightSql.CommandGetPrimaryKeys.newBuilder().setTable(TARGET_TABLE).build();
    final Consumer<FlightProducer.ServerStreamListener> commandGetPrimaryKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_PRIMARY_KEYS_SCHEMA,
               allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector columnName = (VarCharVector) root.getVector("column_name");
        final IntVector keySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector keyName = (VarCharVector) root.getVector("key_name");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("db_schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .peek(i -> columnName.setSafe(i, new Text(format("column_name #%d", i))))
            .peek(i -> keySequence.setSafe(i, i))
            .forEach(i -> keyName.setSafe(i, new Text(format("key_name #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetPrimaryKeys, commandGetPrimaryKeysResultProducer);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(connection, FLIGHT_SQL_PRODUCER, allocator);
  }


  @Test
  public void testGetCatalogsCanBeAccessedByIndices() throws IOException, AdbcException {
    try (final ArrowReader resultSet = connection.getObjects(AdbcConnection.GetObjectsDepth.CATALOGS, null, null, null, null, null)) {
/*
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_CATALOGS_RESULTS);
*/
    }
  }

  @Test
  public void testGetTablesCanBeAccessedByIndices() throws IOException, AdbcException {
    try (final ArrowReader resultSet = connection.getObjects(AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)) {
/*
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_TABLES_RESULTS);
*/
    }
  }

  @Test
  public void testGetSchemasCanBeAccessedByIndices() throws IOException, AdbcException {
    try (final ArrowReader resultSet = connection.getObjects(AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
/*
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_SCHEMAS_RESULTS);
*/
    }
  }

  @Test
  public void testGetColumnsCanBeAccessedByIndices() throws IOException, AdbcException {
    try (final ArrowReader resultSet = connection.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, null)) {
/*
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_COLUMNS_RESULTS);
*/
    }
  }

  @Test
  public void testGetColumnsCanByIndicesFilteringColumnNames() throws IOException, AdbcException {
    try (final ArrowReader resultSet = connection.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, "column_1")) {
/*
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_COLUMNS_RESULTS
          .stream()
          .filter(insideList -> Objects.equals(insideList.get(3), "column_1"))
          .collect(toList())
      );
*/
    }
  }
}
