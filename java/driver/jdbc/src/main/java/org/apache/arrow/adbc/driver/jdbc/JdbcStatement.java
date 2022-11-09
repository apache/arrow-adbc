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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class JdbcStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final Connection connection;
  private final SqlQuirks quirks;

  // State for SQL queries
  private Statement statement;
  private String sqlQuery;
  private ArrowReader reader;
  private ResultSet resultSet;
  // State for bulk ingest
  private BulkState bulkOperation;
  private VectorSchemaRoot bindRoot;

  JdbcStatement(BufferAllocator allocator, Connection connection, SqlQuirks quirks) {
    this.allocator = allocator;
    this.connection = connection;
    this.quirks = quirks;
    this.sqlQuery = null;
  }

  static JdbcStatement ingestRoot(
      BufferAllocator allocator,
      Connection connection,
      SqlQuirks quirks,
      String targetTableName,
      BulkIngestMode mode) {
    Objects.requireNonNull(targetTableName);
    final JdbcStatement statement = new JdbcStatement(allocator, connection, quirks);
    statement.bulkOperation = new BulkState();
    statement.bulkOperation.mode = mode;
    statement.bulkOperation.targetTable = targetTableName;
    return statement;
  }

  @Override
  public void setSqlQuery(String query) throws AdbcException {
    if (bulkOperation != null) {
      throw AdbcException.invalidState(
          "[JDBC] Statement is configured for a bulk ingest/append operation");
    }
    sqlQuery = query;
  }

  @Override
  public void bind(VectorSchemaRoot root) {
    bindRoot = root;
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
      create.append(' ');
      String typeName = quirks.getArrowToSqlTypeNameMapping().apply(field.getType());
      if (typeName == null) {
        throw AdbcException.notImplemented(
            "[JDBC] Cannot generate CREATE TABLE statement for field " + field);
      }
      create.append(typeName);
    }
    create.append(")");

    try (final Statement statement = connection.createStatement()) {
      statement.execute(create.toString());
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(
          AdbcStatusCode.ALREADY_EXISTS,
          "Could not create table %s: ",
          e,
          bulkOperation.targetTable);
    }
  }

  private UpdateResult executeBulk() throws AdbcException {
    if (bindRoot == null) {
      throw AdbcException.invalidState("[JDBC] Must call bind() before bulk insert");
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
      throw JdbcDriverUtil.fromSqlException(
          "Could not bulk insert into table %s: ", e, bulkOperation.targetTable);
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
    return new UpdateResult(bindRoot.getRowCount());
  }

  private void invalidatePriorQuery() throws AdbcException {
    try {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          throw new AdbcException(
              "[JDBC] Failed to close unread result set",
              e,
              AdbcStatusCode.IO,
              null, /*vendorCode*/
              0);
        }
        reader = null;
      }
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
      }
      if (!(statement instanceof PreparedStatement) && statement != null) {
        statement.close();
        statement = null;
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public UpdateResult executeUpdate() throws AdbcException {
    if (bulkOperation != null) {
      return executeBulk();
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[JDBC] Must setSqlQuery() first");
    }
    long affectedRows = 0;
    try {
      invalidatePriorQuery();
      if (statement instanceof PreparedStatement) {
        PreparedStatement preparedStatement = (PreparedStatement) statement;
        if (bindRoot != null) {
          final JdbcParameterBinder binder =
              JdbcParameterBinder.builder(preparedStatement, bindRoot).bindAll().build();
          preparedStatement.clearBatch();
          while (binder.next()) {
            preparedStatement.addBatch();
          }
          affectedRows = LongStream.of(preparedStatement.executeLargeBatch()).sum();
        } else {
          affectedRows = preparedStatement.executeUpdate();
        }
      } else {
        statement =
            connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        affectedRows = statement.executeUpdate(sqlQuery);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    return new UpdateResult(affectedRows);
  }

  @Override
  public QueryResult executeQuery() throws AdbcException {
    if (bulkOperation != null) {
      throw AdbcException.invalidState("[JDBC] Call executeUpdate() for bulk operations");
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[JDBC] Must setSqlQuery() first");
    }
    try {
      invalidatePriorQuery();
      if (statement instanceof PreparedStatement) {
        PreparedStatement preparedStatement = (PreparedStatement) statement;
        if (bindRoot != null) {
          reader = new JdbcBindReader(allocator, preparedStatement, bindRoot);
        } else {
          resultSet = preparedStatement.executeQuery();
          reader = new JdbcArrowReader(allocator, resultSet, /*overrideSchema*/ null);
        }
      } else {
        statement =
            connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        resultSet = statement.executeQuery(sqlQuery);
        reader = new JdbcArrowReader(allocator, resultSet, /*overrideSchema*/ null);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    return new QueryResult(/*affectedRows=*/ -1, reader);
  }

  @Override
  public Schema getParameterSchema() throws AdbcException {
    if (statement instanceof PreparedStatement) {
      final PreparedStatement preparedStatement = (PreparedStatement) statement;
      try {
        final ParameterMetaData md = preparedStatement.getParameterMetaData();
        final List<Field> fields = new ArrayList<>(md.getParameterCount());
        for (int i = 0; i < md.getParameterCount(); i++) {
          final int paramIndex = i + 1;
          JdbcFieldInfo fieldInfo =
              new JdbcFieldInfo(
                  md.getParameterType(paramIndex),
                  md.getPrecision(paramIndex),
                  md.getScale(paramIndex));
          ArrowType arrowType =
              JdbcToArrowUtils.getArrowTypeFromJdbcType(
                  fieldInfo, JdbcToArrowUtils.getUtcCalendar());
          fields.add(Field.nullable("", arrowType));
        }
        return new Schema(fields);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    throw AdbcException.invalidState("[JDBC] Must prepare() before getParameterSchema()");
  }

  @Override
  public void prepare() throws AdbcException {
    try {
      if (sqlQuery == null) {
        throw AdbcException.invalidArgument("[JDBC] Must setSqlQuery(String) before prepare()");
      }
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
