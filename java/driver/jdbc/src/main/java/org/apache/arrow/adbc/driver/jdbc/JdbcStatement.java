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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.util.JdbcParameterBinder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

public class JdbcStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final Connection connection;

  // State for SQL queries
  private Statement statement;
  private String sqlQuery;
  private ResultSet resultSet;
  // State for bulk ingest
  private String bulkTargetTable;
  private VectorSchemaRoot bindRoot;

  JdbcStatement(BufferAllocator allocator, Connection connection) {
    this.allocator = allocator;
    this.connection = connection;
    this.sqlQuery = null;
  }

  static AdbcStatement ingestRoot(
      BufferAllocator allocator, Connection connection, String targetTableName) {
    Objects.requireNonNull(targetTableName);
    final JdbcStatement statement = new JdbcStatement(allocator, connection);
    statement.bulkTargetTable = targetTableName;
    return statement;
  }

  @Override
  public void setSqlQuery(String query) {
    bulkTargetTable = null;
    sqlQuery = query;
  }

  @Override
  public void bind(VectorSchemaRoot root) {
    bindRoot = root;
  }

  @Override
  public void execute() throws AdbcException {
    if (bulkTargetTable != null) {
      executeBulk();
    } else if (sqlQuery != null) {
      executeSqlQuery();
    } else {
      throw new IllegalStateException("Must setSqlQuery first");
    }
  }

  private void executeBulk() throws AdbcException {
    if (bindRoot == null) {
      throw new IllegalStateException("Must bind() before bulk insert");
    }

    // TODO: also create the table
    final StringBuilder create = new StringBuilder("CREATE TABLE ");
    create.append(bulkTargetTable);
    create.append(" (");
    for (int col = 0; col < bindRoot.getFieldVectors().size(); col++) {
      if (col > 0) {
        create.append(", ");
      }
      final Field field = bindRoot.getVector(col).getField();
      create.append(field.getName());
      switch (field.getType().getTypeID()) {
        case Null:
        case Struct:
        case List:
        case LargeList:
        case FixedSizeList:
        case Union:
        case Map:
          throw new UnsupportedOperationException("Type " + field);
        case Int:
          // TODO:
          create.append(" INT");
          break;
        case FloatingPoint:
          throw new UnsupportedOperationException("Type " + field);
        case Utf8:
          create.append(" CLOB");
          break;
        case LargeUtf8:
        case Binary:
        case LargeBinary:
        case FixedSizeBinary:
        case Bool:
        case Decimal:
        case Date:
        case Time:
        case Timestamp:
        case Interval:
        case Duration:
        case NONE:
          throw new UnsupportedOperationException("Type " + field);
      }
    }
    create.append(")");

    try (final Statement statement = connection.createStatement()) {
      statement.execute(create.toString());
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }

    // XXX: potential injection
    // TODO: consider (optionally?) depending on jOOQ to generate SQL and support different dialects
    final StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(bulkTargetTable);
    insert.append(" VALUES (");
    for (int col = 0; col < bindRoot.getFieldVectors().size(); col++) {
      if (col > 0) {
        insert.append(", ");
      }
      insert.append("?");
    }
    insert.append(")");

    try (final PreparedStatement statement = connection.prepareStatement(insert.toString())) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, bindRoot).bindAll().build();
      statement.clearBatch();
      while (binder.next()) {
        statement.addBatch();
      }
      statement.executeBatch();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  private void executeSqlQuery() throws AdbcException {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      statement =
          connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      resultSet = statement.executeQuery(sqlQuery);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public ArrowReader getArrowReader() throws AdbcException {
    if (resultSet == null) {
      throw new IllegalStateException("Must call execute() before getArrowIterator()");
    }
    final JdbcArrowReader reader = new JdbcArrowReader(allocator, resultSet);
    resultSet = null;
    return reader;
  }

  @Override
  public void prepare() {
    throw new UnsupportedOperationException("prepare");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(resultSet, statement);
  }
}
