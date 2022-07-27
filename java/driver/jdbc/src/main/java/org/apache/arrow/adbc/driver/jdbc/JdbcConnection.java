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
import java.util.List;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class JdbcConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final Connection connection;
  private final SqlQuirks quirks;

  JdbcConnection(BufferAllocator allocator, Connection connection, SqlQuirks quirks) {
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
  public AdbcStatement getInfo(int[] infoCodes) throws AdbcException {
    try {
      final VectorSchemaRoot root =
          new InfoMetadataBuilder(allocator, connection, infoCodes).build();
      return new FixedJdbcStatement(allocator, root);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public AdbcStatement getObjects(
      final GetObjectsDepth depth,
      final String catalogPattern,
      final String dbSchemaPattern,
      final String tableNamePattern,
      final String[] tableTypes,
      final String columnNamePattern)
      throws AdbcException {
    // Build up the metadata in-memory and then return a constant reader.
    try {
      final VectorSchemaRoot root =
          new ObjectMetadataBuilder(
                  allocator,
                  connection,
                  depth,
                  catalogPattern,
                  dbSchemaPattern,
                  tableNamePattern,
                  tableTypes,
                  columnNamePattern)
              .build();
      return new FixedJdbcStatement(allocator, root);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public Schema getTableSchema(String catalog, String dbSchema, String tableName)
      throws AdbcException {
    // Reconstruct the schema from the metadata
    // XXX: this may be inconsistent with reading the table
    final List<Field> fields = new ArrayList<>();
    try (final ResultSet rs =
        connection
            .getMetaData()
            .getColumns(catalog, dbSchema, tableName, /*columnNamePattern*/ null)) {
      while (rs.next()) {
        final String fieldName = rs.getString("COLUMN_NAME");
        final boolean nullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
        final int jdbcType = rs.getInt("DATA_TYPE");
        final int precision = rs.getInt("COLUMN_SIZE");
        final int scale = rs.getInt("DECIMAL_DIGITS");
        final ArrowType arrowType =
            JdbcToArrowUtils.getArrowTypeFromJdbcType(
                new JdbcFieldInfo(jdbcType, precision, scale), /*calendar*/ null);
        final Field field =
            new Field(
                fieldName,
                new FieldType(
                    nullable, arrowType, /*dictionary*/ null, /*metadata*/ null), /*children*/
                null);
        fields.add(field);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    return new Schema(fields);
  }

  @Override
  public AdbcStatement getTableTypes() throws AdbcException {
    // XXX: this is nonconforming since we don't currently have a way to rename the columns
    try {
      return new FixedJdbcStatement(
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
  public void close() throws Exception {
    connection.close();
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
