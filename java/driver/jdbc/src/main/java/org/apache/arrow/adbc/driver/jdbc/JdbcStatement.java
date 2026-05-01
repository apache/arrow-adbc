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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
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
import org.checkerframework.checker.nullness.qual.Nullable;

public class JdbcStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final Connection connection;
  private final JdbcQuirks quirks;

  // State for SQL queries
  private @Nullable Statement statement;
  private @Nullable String sqlQuery;
  private @Nullable ArrowReader reader;
  private @Nullable ResultSet resultSet;
  // State for bulk ingest
  private @Nullable BulkState bulkOperation;
  private @Nullable VectorSchemaRoot bindRoot;

  JdbcStatement(BufferAllocator allocator, Connection connection, JdbcQuirks quirks) {
    this.allocator = allocator;
    this.connection = connection;
    this.quirks = quirks;
    this.sqlQuery = null;
  }

  static JdbcStatement ingestRoot(
      BufferAllocator allocator,
      Connection connection,
      JdbcQuirks quirks,
      String targetTableName,
      BulkIngestMode mode) {
    Objects.requireNonNull(targetTableName);
    final JdbcStatement statement = new JdbcStatement(allocator, connection, quirks);
    statement.bulkOperation = new BulkState(mode, targetTableName);
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

  private void createBulkTable(BulkState bulkOperation, VectorSchemaRoot bindRoot)
      throws AdbcException {
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
      @Nullable SqlQuirks sqlQuirks = quirks.getSqlQuirks();
      if (sqlQuirks == null) {
        throw AdbcException.invalidState(
            JdbcDriverUtil.prefixExceptionMessage(
                "Must create driver with SqlQuirks to use bulk ingestion"));
      }
      String typeName = sqlQuirks.getArrowToSqlTypeNameMapping().apply(field.getType());
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

  private UpdateResult executeBulk(BulkState bulkOperation) throws AdbcException {
    if (bindRoot == null) {
      throw AdbcException.invalidState("[JDBC] Must call bind() before bulk insert");
    }
    VectorSchemaRoot bind = bindRoot;

    if (bulkOperation.mode == BulkIngestMode.CREATE) {
      createBulkTable(bulkOperation, bind);
    }

    // XXX: potential injection
    // TODO: consider (optionally?) depending on jOOQ to generate SQL and support different dialects
    final StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(bulkOperation.targetTable);
    insert.append(" VALUES (");
    for (int col = 0; col < bind.getFieldVectors().size(); col++) {
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
            JdbcParameterBinder.builder(statement, bind).bindAll().build();
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
    return new UpdateResult(bind.getRowCount());
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
      return executeBulk(bulkOperation);
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[JDBC] Must setSqlQuery() first");
    }
    String query = sqlQuery;

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
        affectedRows = statement.executeUpdate(query);
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
    String query = sqlQuery;

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
        resultSet = statement.executeQuery(query);
        reader = new JdbcArrowReader(allocator, resultSet, /*overrideSchema*/ null);
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    return new QueryResult(/* affectedRows */ -1, reader);
  }

  @Override
  public Schema executeSchema() throws AdbcException {
    if (bulkOperation != null) {
      throw AdbcException.invalidState("[JDBC] Call executeUpdate() for bulk operations");
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[JDBC] Must setSqlQuery() first");
    }
    String query = sqlQuery;

    try {
      invalidatePriorQuery();
      final PreparedStatement preparedStatement;
      final PreparedStatement ownedStatement;
      if (statement instanceof PreparedStatement) {
        preparedStatement = (PreparedStatement) statement;
        if (bindRoot != null) {
          JdbcParameterBinder.builder(preparedStatement, bindRoot).bindAll().build().next();
        }
        ownedStatement = null;
      } else {
        // new statement
        preparedStatement = connection.prepareStatement(query);
        ownedStatement = preparedStatement;
      }

      final JdbcToArrowConfig config = JdbcArrowReader.makeJdbcConfig(allocator);
      @Nullable ResultSetMetaData rsmd = preparedStatement.getMetaData();
      if (rsmd == null) {
        throw new AdbcException(
            JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null ResultSetMetaData"),
            /*cause*/ null,
            AdbcStatusCode.INTERNAL,
            null,
            0);
      }
      final Schema schema = JdbcToArrowUtils.jdbcToArrowSchema(rsmd, config);
      if (ownedStatement != null) {
        ownedStatement.close();
      }
      return schema;
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
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
      String query = sqlQuery;
      if (resultSet != null) {
        resultSet.close();
      }

      statement =
          connection.prepareStatement(
              query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void close() throws AdbcException {
    try {
      AutoCloseables.close(reader, resultSet, statement);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw AdbcException.io(e);
    }
  }

  private static final class BulkState {
    BulkIngestMode mode;
    String targetTable;

    BulkState(BulkIngestMode mode, String targetTable) {
      this.mode = mode;
      this.targetTable = targetTable;
    }
  }
}
