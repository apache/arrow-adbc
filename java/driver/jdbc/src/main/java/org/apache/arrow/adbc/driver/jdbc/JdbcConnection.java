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
package org.apache.arrow.adbc.driver.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.IsolationLevel;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.adbc.core.StandardStatistics;
import org.apache.arrow.adbc.driver.jdbc.adapter.JdbcFieldInfoExtra;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.checkerframework.checker.nullness.qual.Nullable;

public class JdbcConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final Connection connection;
  private final JdbcQuirks quirks;

  /**
   * Create a new connection.
   *
   * @param allocator The allocator to use. The connection will close the allocator when done.
   * @param connection The JDBC connection.
   * @param quirks Backend-specific quirks to account for.
   */
  JdbcConnection(BufferAllocator allocator, Connection connection, JdbcQuirks quirks) {
    this.allocator = allocator;
    this.connection = connection;
    this.quirks = quirks;
  }

  @Override
  public void commit() throws AdbcException {
    try {
      checkAutoCommit();
      connection.commit();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public AdbcStatement createStatement() throws AdbcException {
    return new JdbcStatement(allocator, connection, quirks);
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    return JdbcStatement.ingestRoot(allocator, connection, quirks, targetTableName, mode);
  }

  @Override
  public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
    try {
      try (final VectorSchemaRoot root =
          new InfoMetadataBuilder(allocator, connection, infoCodes).build()) {
        return RootArrowReader.fromRoot(allocator, root);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public ArrowReader getObjects(
      final GetObjectsDepth depth,
      final String catalogPattern,
      final String dbSchemaPattern,
      final String tableNamePattern,
      final String[] tableTypes,
      final String columnNamePattern)
      throws AdbcException {
    // Build up the metadata in-memory and then return a constant reader.
    try (final VectorSchemaRoot root =
        new ObjectMetadataBuilder(
                allocator,
                connection,
                depth,
                catalogPattern,
                dbSchemaPattern,
                tableNamePattern,
                tableTypes,
                columnNamePattern)
            .build()) {
      return RootArrowReader.fromRoot(allocator, root);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  static final class Statistic {
    String table;
    String column;
    short key;
    long value;
    boolean multiColumn = false;

    public Statistic(String table, String column) {
      this.table = table;
      this.column = column;
    }
  }

  // We use null keys intentionally with HashMap, but the annotations don't technically allow this
  @SuppressWarnings("argument")
  @Override
  public ArrowReader getStatistics(
      String catalogPattern, String dbSchemaPattern, String tableNamePattern, boolean approximate)
      throws AdbcException {
    if (tableNamePattern == null) {
      throw AdbcException.notImplemented(
          JdbcDriverUtil.prefixExceptionMessage("getStatistics: must supply table name"));
    }

    try (final VectorSchemaRoot root =
            VectorSchemaRoot.create(StandardSchemas.GET_STATISTICS_SCHEMA, allocator);
        ResultSet rs =
            connection
                .getMetaData()
                .getIndexInfo(
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern, /*unique*/
                    false,
                    approximate)) {
      // Build up the statistics in-memory and then return a constant reader.
      // We have to read and sort the data first because the ordering is not by the catalog/etc.

      // {catalog: {schema: {index_name: statistic}}}
      Map<String, Map<String, Map<String, Statistic>>> allStatistics = new HashMap<>();

      while (rs.next()) {
        @Nullable String catalog = rs.getString(1);
        String schema = rs.getString(2);
        String table = rs.getString(3);
        String index = rs.getString(6);
        short statisticType = rs.getShort(7);
        String column = rs.getString(9);
        long cardinality = rs.getLong(11);

        if (table == null || column == null) {
          throw new AdbcException(
              JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null table/column name"),
              null,
              AdbcStatusCode.INTERNAL,
              null,
              0);
        }

        if (!allStatistics.containsKey(catalog)) {
          allStatistics.put(catalog, new HashMap<>());
        }

        Map<String, Map<String, Statistic>> catalogStats = allStatistics.get(catalog);
        if (!catalogStats.containsKey(schema)) {
          catalogStats.put(schema, new HashMap<>());
        }

        Map<String, Statistic> schemaStats = catalogStats.get(schema);
        Statistic statistic = schemaStats.getOrDefault(index, new Statistic(table, column));
        assert statistic != null; // for checker-framework
        if (schemaStats.containsKey(index)) {
          // Multi-column index, ignore it
          statistic.multiColumn = true;
          continue;
        }

        statistic.column = column;
        statistic.table = table;
        statistic.key =
            statisticType == DatabaseMetaData.tableIndexStatistic
                ? StandardStatistics.ROW_COUNT.getKey()
                : StandardStatistics.DISTINCT_COUNT.getKey();
        statistic.value = cardinality;
        schemaStats.put(index, statistic);
      }

      VarCharVector catalogNames = (VarCharVector) root.getVector(0);
      ListVector catalogDbSchemas = (ListVector) root.getVector(1);
      StructVector dbSchemas = (StructVector) catalogDbSchemas.getDataVector();
      VarCharVector dbSchemaNames = (VarCharVector) dbSchemas.getVectorById(0);
      ListVector dbSchemaStatistics = (ListVector) dbSchemas.getVectorById(1);
      StructVector statistics = (StructVector) dbSchemaStatistics.getDataVector();
      VarCharVector tableNames = (VarCharVector) statistics.getVectorById(0);
      VarCharVector columnNames = (VarCharVector) statistics.getVectorById(1);
      SmallIntVector statisticKeys = (SmallIntVector) statistics.getVectorById(2);
      DenseUnionVector statisticValues = (DenseUnionVector) statistics.getVectorById(3);
      BitVector statisticIsApproximate = (BitVector) statistics.getVectorById(4);

      // Build up the Arrow result
      Text text = new Text();
      NullableBigIntHolder holder = new NullableBigIntHolder();
      int catalogIndex = 0;
      int schemaIndex = 0;
      int statisticIndex = 0;
      for (String catalog : allStatistics.keySet()) {
        Map<String, Map<String, Statistic>> schemas = allStatistics.get(catalog);

        if (catalog == null) {
          catalogNames.setNull(catalogIndex);
        } else {
          text.set(catalog);
          catalogNames.setSafe(catalogIndex, text);
        }
        catalogDbSchemas.startNewValue(catalogIndex);

        int schemaCount = 0;
        for (String schema : schemas.keySet()) {
          if (schema == null) {
            dbSchemaNames.setNull(schemaIndex);
          } else {
            text.set(schema);
            dbSchemaNames.setSafe(schemaIndex, text);
          }

          dbSchemaStatistics.startNewValue(schemaIndex);

          Map<String, Statistic> indices = schemas.get(schema);
          int statisticCount = 0;
          for (Statistic statistic : indices.values()) {
            if (statistic.multiColumn) {
              continue;
            }

            text.set(statistic.table);
            tableNames.setSafe(statisticIndex, text);
            if (statistic.column == null) {
              columnNames.setNull(statisticIndex);
            } else {
              text.set(statistic.column);
              columnNames.setSafe(statisticIndex, text);
            }
            statisticKeys.setSafe(statisticIndex, statistic.key);
            statisticValues.setTypeId(statisticIndex, (byte) 0);
            holder.isSet = 1;
            holder.value = statistic.value;
            statisticValues.setSafe(statisticIndex, holder);
            statisticIsApproximate.setSafe(statisticIndex, approximate ? 1 : 0);

            statistics.setIndexDefined(statisticIndex++);
            statisticCount++;
          }

          dbSchemaStatistics.endValue(schemaIndex, statisticCount);

          dbSchemas.setIndexDefined(schemaIndex++);
          schemaCount++;
        }

        catalogDbSchemas.endValue(catalogIndex, schemaCount);
        catalogIndex++;
      }
      root.setRowCount(catalogIndex);

      return RootArrowReader.fromRoot(allocator, root);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public ArrowReader getStatisticNames() throws AdbcException {
    // TODO:
    return AdbcConnection.super.getStatisticNames();
  }

  @Override
  public Schema getTableSchema(String catalog, String dbSchema, String tableName)
      throws AdbcException {
    // Check for existence
    // XXX: this is TOC/TOU error, but without an explicit check we just get no fields (possibly we
    // should just assume it's impossible to have a 0-column table and error there?)
    try (final ResultSet rs =
        connection.getMetaData().getTables(catalog, dbSchema, tableName, /*tableTypes*/ null)) {
      if (!rs.next()) {
        throw new AdbcException(
            JdbcDriverUtil.prefixExceptionMessage("Table not found: " + tableName), /*cause*/
            null,
            AdbcStatusCode.NOT_FOUND, /*sqlState*/
            null, /*vendorCode*/
            0);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }

    // Reconstruct the schema from the metadata
    // XXX: this may be inconsistent with reading the table
    final List<Field> fields = new ArrayList<>();
    try (final ResultSet rs =
        connection
            .getMetaData()
            .getColumns(catalog, dbSchema, tableName, /*columnNamePattern*/ null)) {
      while (rs.next()) {
        @Nullable String fieldName = rs.getString("COLUMN_NAME");
        if (fieldName == null) {
          fieldName = "";
        }
        final JdbcFieldInfoExtra fieldInfoExtra = new JdbcFieldInfoExtra(rs);

        final ArrowType arrowType = quirks.getTypeConverter().apply(fieldInfoExtra);
        if (arrowType == null) {
          throw AdbcException.notImplemented(
              JdbcDriverUtil.prefixExceptionMessage(
                  String.format(
                      "Column '%s' has unsupported type: %s", fieldName, fieldInfoExtra)));
        }

        final Field field =
            new Field(
                fieldName,
                fieldInfoExtra.isNullable() == DatabaseMetaData.columnNoNulls
                    ? FieldType.notNullable(arrowType)
                    : FieldType.nullable(arrowType),
                Collections.emptyList());
        fields.add(field);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    return new Schema(fields);
  }

  @Override
  public ArrowReader getTableTypes() throws AdbcException {
    try {
      return new JdbcArrowReader(
          allocator, connection.getMetaData().getTableTypes(), StandardSchemas.TABLE_TYPES_SCHEMA);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void rollback() throws AdbcException {
    try {
      checkAutoCommit();
      connection.rollback();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public boolean getAutoCommit() throws AdbcException {
    try {
      return connection.getAutoCommit();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void setAutoCommit(boolean enableAutoCommit) throws AdbcException {
    try {
      connection.setAutoCommit(enableAutoCommit);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public String getCurrentCatalog() throws AdbcException {
    try {
      return connection.getCatalog();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void setCurrentCatalog(String catalog) throws AdbcException {
    try {
      connection.setCatalog(catalog);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public String getCurrentDbSchema() throws AdbcException {
    try {
      return connection.getSchema();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void setCurrentDbSchema(String dbSchema) throws AdbcException {
    try {
      connection.setSchema(dbSchema);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public boolean getReadOnly() throws AdbcException {
    try {
      return connection.isReadOnly();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void setReadOnly(boolean isReadOnly) throws AdbcException {
    try {
      connection.setReadOnly(isReadOnly);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public IsolationLevel getIsolationLevel() throws AdbcException {
    try {
      final int transactionIsolation = connection.getTransactionIsolation();
      switch (transactionIsolation) {
        case Connection.TRANSACTION_NONE:
          return IsolationLevel.DEFAULT;
        case Connection.TRANSACTION_READ_UNCOMMITTED:
          return IsolationLevel.READ_UNCOMMITTED;
        case Connection.TRANSACTION_READ_COMMITTED:
          return IsolationLevel.READ_COMMITTED;
        case Connection.TRANSACTION_REPEATABLE_READ:
          return IsolationLevel.REPEATABLE_READ;
        case Connection.TRANSACTION_SERIALIZABLE:
          return IsolationLevel.SERIALIZABLE;
        default:
          throw AdbcException.notImplemented(
              JdbcDriverUtil.prefixExceptionMessage(
                  "JDBC isolation level not recognized: " + transactionIsolation));
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void setIsolationLevel(IsolationLevel level) throws AdbcException {
    try {
      switch (level) {
        case READ_UNCOMMITTED:
          connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
          break;
        case READ_COMMITTED:
          connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          break;
        case REPEATABLE_READ:
          connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          break;
        case SERIALIZABLE:
          connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
          break;
        case DEFAULT:
        case SNAPSHOT:
        case LINEARIZABLE:
          throw AdbcException.notImplemented(
              JdbcDriverUtil.prefixExceptionMessage("Isolation level not supported: " + level));
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void close() throws AdbcException {
    try {
      AutoCloseables.close(connection, allocator);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw AdbcException.io(e);
    }
  }

  private void checkAutoCommit() throws AdbcException, SQLException {
    if (connection.getAutoCommit()) {
      throw AdbcException.invalidState("[JDBC] Cannot perform operation in autocommit mode");
    }
  }

  @Override
  public String toString() {
    return "JdbcConnection{" + "connection=" + connection + '}';
  }
}
