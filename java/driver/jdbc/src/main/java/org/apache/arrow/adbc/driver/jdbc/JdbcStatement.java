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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.driver.jdbc.util.JdbcParameterBinder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

public class JdbcStatement implements AdbcStatement {
  // Do our best to properly map database-specific errors to NOT_FOUND status.
  private static final List<String> SQLSTATE_TABLE_NOT_FOUND =
      Arrays.asList(
          // Apache Derby https://db.apache.org/derby/docs/10.4/ref/rrefexcept71493.html
          "42X05",
          // MySQL
          // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
          "42S02",
          // Postgres https://www.postgresql.org/docs/current/errcodes-appendix.html
          "42P01");
  private final BufferAllocator allocator;
  private final Connection connection;

  // State for SQL queries
  private Statement statement;
  private String sqlQuery;
  private ArrowReader reader;
  private ResultSet resultSet;
  // State for bulk ingest
  private BulkState bulkOperation;
  private VectorSchemaRoot bindRoot;

  JdbcStatement(BufferAllocator allocator, Connection connection) {
    this.allocator = allocator;
    this.connection = connection;
    this.sqlQuery = null;
  }

  static JdbcStatement ingestRoot(
      BufferAllocator allocator,
      Connection connection,
      String targetTableName,
      BulkIngestMode mode) {
    Objects.requireNonNull(targetTableName);
    final JdbcStatement statement = new JdbcStatement(allocator, connection);
    statement.bulkOperation = new BulkState();
    statement.bulkOperation.mode = mode;
    statement.bulkOperation.targetTable = targetTableName;
    return statement;
  }

  @Override
  public void setSqlQuery(String query) {
    if (bulkOperation != null) {
      throw new IllegalStateException("Statement is configured for a bulk ingest/append operation");
    }
    sqlQuery = query;
  }

  @Override
  public void bind(VectorSchemaRoot root) {
    bindRoot = root;
  }

  @Override
  public void execute() throws AdbcException {
    if (bulkOperation != null) {
      executeBulk();
    } else if (sqlQuery != null) {
      executeSqlQuery();
    } else {
      throw new IllegalStateException("Must setSqlQuery first");
    }
  }

  private void createBulkTable() throws AdbcException {
    final StringBuilder create = new StringBuilder("CREATE TABLE ");
    create.append(bulkOperation.targetTable);
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
      throw JdbcDriverUtil.fromSqlException(
          AdbcStatusCode.ALREADY_EXISTS, "Could not create table %s", e, bulkOperation.targetTable);
    }
  }

  private void executeBulk() throws AdbcException {
    if (bindRoot == null) {
      throw new IllegalStateException("Must bind() before bulk insert");
    }

    if (bulkOperation.mode == BulkIngestMode.CREATE) {
      createBulkTable();
    }

    // XXX: potential injection
    // TODO: consider (optionally?) depending on jOOQ to generate SQL and support different dialects
    final StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(bulkOperation.targetTable);
    insert.append(" VALUES (");
    for (int col = 0; col < bindRoot.getFieldVectors().size(); col++) {
      if (col > 0) {
        insert.append(", ");
      }
      insert.append("?");
    }
    insert.append(")");

    final PreparedStatement statement;
    try {
      statement = connection.prepareStatement(insert.toString());
    } catch (SQLException e) {
      // It's hard to differentiate between 'table not found' and parameter type/count mismatch here
      // because SQLState is inconsistent (see SQLSTATE_TABLE_NOT_FOUND above). We could query for
      // table existence but that's another roundtrip and leads to a TOC/TOU
      // error. Instead, we hard-code some common codes here.

      final AdbcStatusCode code =
          SQLSTATE_TABLE_NOT_FOUND.contains(e.getSQLState())
              ? AdbcStatusCode.NOT_FOUND
              : AdbcStatusCode.ALREADY_EXISTS;
      throw JdbcDriverUtil.fromSqlException(
          code, "Could not bulk insert into table %s: ", e, bulkOperation.targetTable);
    }
    try {
      try {
        final JdbcParameterBinder binder =
            JdbcParameterBinder.builder(statement, bindRoot).bindAll().build();
        statement.clearBatch();
        while (binder.next()) {
          statement.addBatch();
        }
        statement.executeBatch();
      } finally {
        statement.close();
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  private void executeSqlQuery() throws AdbcException {
    try {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          throw new AdbcException(
              "Failed to close unread result set", e, AdbcStatusCode.IO, null, /*vendorCode*/ 0);
        }
      }
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement instanceof PreparedStatement) {
        PreparedStatement preparedStatement = (PreparedStatement) statement;
        if (bindRoot != null) {
          reader = new JdbcBindReader(allocator, preparedStatement, bindRoot);
        } else {
          resultSet = preparedStatement.executeQuery();
        }
      } else {
        if (statement != null) {
          statement.close();
        }
        statement =
            connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        resultSet = statement.executeQuery(sqlQuery);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public ArrowReader getArrowReader() throws AdbcException {
    if (reader != null) {
      ArrowReader result = reader;
      reader = null;
      return result;
    }
    if (resultSet == null) {
      throw new IllegalStateException("Must call execute() before getArrowIterator()");
    }
    final JdbcArrowReader reader =
        new JdbcArrowReader(allocator, resultSet, /*overrideSchema*/ null);
    resultSet = null;
    return reader;
  }

  @Override
  public void prepare() throws AdbcException {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      statement =
          connection.prepareStatement(
              sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(reader, resultSet, statement);
  }

  private static final class BulkState {
    public BulkIngestMode mode;
    String targetTable;
  }
}
