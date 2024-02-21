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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper class to track state needed to build up the object metadata structure. */
final class ObjectMetadataBuilder implements AutoCloseable {
  private final AdbcConnection.GetObjectsDepth depth;
  private final Predicate<String> catalogPattern;
  private final String dbSchemaPattern;
  private final String tableNamePattern;
  private final String[] tableTypesFilter;
  private final String columnNamePattern;
  private final DatabaseMetaData dbmd;
  private VectorSchemaRoot root;

  final VarCharVector catalogNames;
  final ListVector catalogDbSchemas;
  final StructVector dbSchemas;
  final VarCharVector dbSchemaNames;
  final ListVector dbSchemaTables;
  final StructVector tables;
  final VarCharVector tableNames;
  final VarCharVector tableTypes;
  final ListVector tableColumns;
  final StructVector columns;
  final VarCharVector columnNames;
  final IntVector columnOrdinalPositions;
  final VarCharVector columnRemarks;
  final SmallIntVector columnXdbcDataTypes;
  final ListVector tableConstraints;
  final UnionListWriter tableConstraintsWriter;
  final StructWriter tableConstraintsStructWriter;
  final VarCharWriter constraintNamesWriter;
  final VarCharWriter constraintTypesWriter;
  final ListWriter constraintColumnNamesWriter;
  final ListWriter constraintColumnUsageWriter;
  final StructWriter constraintColumnUsageStructWriter;
  final VarCharWriter constraintColumnUsageFkCatalogsWriter;
  final VarCharWriter constraintColumnUsageFkDbSchemasWriter;
  final VarCharWriter constraintColumnUsageFkTablesWriter;
  final VarCharWriter constraintColumnUsageFkColumnsWriter;
  final BufferAllocator allocator;

  ObjectMetadataBuilder(
      BufferAllocator allocator,
      Connection connection,
      final AdbcConnection.GetObjectsDepth depth,
      final String catalogPattern,
      final String dbSchemaPattern,
      final String tableNamePattern,
      final String[] tableTypesFilter,
      final String columnNamePattern)
      throws SQLException {
    this.allocator = allocator;
    this.depth = depth;
    if (catalogPattern == null) {
      this.catalogPattern = (ignored) -> true;
    } else {
      Pattern pattern = Pattern.compile(translatePattern(catalogPattern));
      this.catalogPattern = (catalog) -> pattern.matcher(catalog).matches();
    }
    this.dbSchemaPattern = dbSchemaPattern;
    this.tableNamePattern = tableNamePattern;
    this.tableTypesFilter = tableTypesFilter;
    this.columnNamePattern = columnNamePattern;
    this.root = VectorSchemaRoot.create(StandardSchemas.GET_OBJECTS_SCHEMA, allocator);
    this.dbmd = connection.getMetaData();
    this.catalogNames = (VarCharVector) root.getVector(0);
    this.catalogDbSchemas = (ListVector) root.getVector(1);
    this.dbSchemas = (StructVector) catalogDbSchemas.getDataVector();
    this.dbSchemaNames = (VarCharVector) dbSchemas.getVectorById(0);
    this.dbSchemaTables = (ListVector) dbSchemas.getVectorById(1);
    this.tables = (StructVector) dbSchemaTables.getDataVector();
    this.tableNames = (VarCharVector) tables.getVectorById(0);
    this.tableTypes = (VarCharVector) tables.getVectorById(1);
    this.tableColumns = (ListVector) tables.getVectorById(2);
    this.columns = (StructVector) tableColumns.getDataVector();
    this.columnNames = (VarCharVector) columns.getVectorById(0);
    this.columnOrdinalPositions = (IntVector) columns.getVectorById(1);
    this.columnRemarks = (VarCharVector) columns.getVectorById(2);
    this.columnXdbcDataTypes = (SmallIntVector) columns.getVectorById(3);
    this.tableConstraints = (ListVector) tables.getVectorById(3);
    this.tableConstraintsWriter = this.tableConstraints.getWriter();
    this.tableConstraintsStructWriter = this.tableConstraintsWriter.struct();
    this.constraintNamesWriter = this.tableConstraintsWriter.varChar("constraint_name");
    this.constraintTypesWriter = this.tableConstraintsWriter.varChar("constraint_type");
    this.constraintColumnNamesWriter = this.tableConstraintsWriter.list("constraint_column_names");
    this.constraintColumnUsageWriter = this.tableConstraintsWriter.list("constraint_column_usage");
    this.constraintColumnUsageStructWriter = this.constraintColumnUsageWriter.struct();
    this.constraintColumnUsageFkCatalogsWriter =
        this.constraintColumnUsageStructWriter.varChar("fk_catalog");
    this.constraintColumnUsageFkDbSchemasWriter =
        this.constraintColumnUsageStructWriter.varChar("fk_db_schema");
    this.constraintColumnUsageFkTablesWriter =
        this.constraintColumnUsageStructWriter.varChar("fk_table");
    this.constraintColumnUsageFkColumnsWriter =
        this.constraintColumnUsageStructWriter.varChar("fk_column_name");
  }

  VectorSchemaRoot build() throws AdbcException, SQLException {
    try (final ResultSet rs = dbmd.getCatalogs()) {
      int catalogCount = 0;
      while (rs.next()) {
        final String catalogName = rs.getString(1);
        if (catalogName == null) {
          throw new AdbcException(
              JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null catalog name"),
              null,
              AdbcStatusCode.INVALID_DATA,
              null,
              0);
        }
        if (!catalogPattern.test(catalogName)) continue;
        addCatalogRow(catalogCount, catalogName);
        catalogCount++;
      }
      // Some databases have an anonymous catalog
      if (catalogPattern.test("") && catalogCount == 0) {
        addCatalogRow(catalogCount, /*catalogName*/ "");
        catalogCount++;
      }
      root.setRowCount(catalogCount);
    }
    VectorSchemaRoot result = root;
    try {
      root = VectorSchemaRoot.create(StandardSchemas.GET_OBJECTS_SCHEMA, allocator);
    } catch (RuntimeException e) {
      result.close();
      throw e;
    }
    return result;
  }

  private void addCatalogRow(int rowIndex, String catalogName) throws AdbcException, SQLException {
    catalogNames.setSafe(rowIndex, catalogName.getBytes(StandardCharsets.UTF_8));
    if (depth == AdbcConnection.GetObjectsDepth.CATALOGS) {
      catalogDbSchemas.setNull(rowIndex);
    } else {
      int dbSchemasBaseIndex = catalogDbSchemas.startNewValue(rowIndex);
      final int dbSchemaCount = buildDbSchemas(dbSchemasBaseIndex, catalogName);
      catalogDbSchemas.endValue(rowIndex, dbSchemaCount);
    }
  }

  private int buildDbSchemas(int rowIndex, String catalogName) throws AdbcException, SQLException {
    int dbSchemaCount = 0;
    // TODO: get tables with no schema
    try (final ResultSet rs = dbmd.getSchemas(catalogName, dbSchemaPattern)) {
      while (rs.next()) {
        final String dbSchemaName = rs.getString(1);
        if (dbSchemaName == null) {
          throw new AdbcException(
              JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null schema name"),
              null,
              AdbcStatusCode.INVALID_DATA,
              null,
              0);
        }
        addDbSchemaRow(rowIndex + dbSchemaCount, catalogName, dbSchemaName);
        dbSchemaCount++;
      }
    }
    return dbSchemaCount;
  }

  private void addDbSchemaRow(int rowIndex, String catalogName, String dbSchemaName)
      throws AdbcException, SQLException {
    dbSchemas.setIndexDefined(rowIndex);
    dbSchemaNames.setSafe(rowIndex, dbSchemaName.getBytes(StandardCharsets.UTF_8));
    if (depth == AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
      dbSchemaTables.setNull(rowIndex);
    } else {
      int tableBaseIndex = dbSchemaTables.startNewValue(rowIndex);
      final int tableCount = buildTables(tableBaseIndex, catalogName, dbSchemaName);
      dbSchemaTables.endValue(rowIndex, tableCount);
    }
  }

  private int buildTables(int rowIndex, String catalogName, String dbSchemaName)
      throws AdbcException, SQLException {
    int tableCount = 0;
    try (final ResultSet rs =
        dbmd.getTables(catalogName, dbSchemaName, tableNamePattern, tableTypesFilter)) {

      while (rs.next()) {
        final @Nullable String tableName = rs.getString(3);
        final @Nullable String tableType = rs.getString(4);
        if (tableName == null || tableType == null) {
          throw new AdbcException(
              JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null table name/type"),
              null,
              AdbcStatusCode.INTERNAL,
              null,
              0);
        }

        tables.setIndexDefined(rowIndex + tableCount);
        tableNames.setSafe(rowIndex + tableCount, tableName.getBytes(StandardCharsets.UTF_8));
        tableTypes.setSafe(rowIndex + tableCount, tableType.getBytes(StandardCharsets.UTF_8));
        tableConstraintsWriter.setPosition(rowIndex + tableCount);
        tableConstraintsWriter.startList();

        // JDBC doesn't directly expose constraints. Merge various info methods:
        // 1. Primary keys
        try (final ResultSet pk = dbmd.getPrimaryKeys(catalogName, dbSchemaName, tableName)) {
          String constraintName = null;
          List<@Nullable String> constraintColumns = new ArrayList<>();
          while (pk.next()) {
            constraintName = pk.getString(6);
            String columnName = pk.getString(4);
            int columnIndex = pk.getInt(5);
            while (constraintColumns.size() < columnIndex) constraintColumns.add(null);
            constraintColumns.set(columnIndex - 1, columnName);
          }
          if (!constraintColumns.isEmpty()) {
            addConstraint(
                constraintName, "PRIMARY KEY", constraintColumns, Collections.emptyList());
          }
        }

        // 2. Foreign keys ("imported" keys)
        try (final ResultSet fk = dbmd.getImportedKeys(catalogName, dbSchemaName, tableName)) {
          List<@Nullable String> names = new ArrayList<>();
          List<List<@Nullable String>> columns = new ArrayList<>();
          List<List<ReferencedColumn>> references = new ArrayList<>();
          while (fk.next()) {
            String keyName = fk.getString(12);
            String keyColumn = fk.getString(8);
            int keySeq = fk.getInt(9);
            if (keySeq == 1) {
              names.add(keyName);
              columns.add(new ArrayList<>());
              references.add(new ArrayList<>());
            }
            columns.get(columns.size() - 1).add(keyColumn);
            final @Nullable String fkTableName = fk.getString(3);
            final @Nullable String fkColumnName = fk.getString(4);
            if (fkTableName == null || fkColumnName == null) {
              throw new AdbcException(
                  JdbcDriverUtil.prefixExceptionMessage(
                      "JDBC driver returned null table/column name"),
                  null,
                  AdbcStatusCode.INTERNAL,
                  null,
                  0);
            }
            final ReferencedColumn reference =
                new ReferencedColumn(fk.getString(1), fk.getString(2), fkTableName, fkColumnName);
            references.get(references.size() - 1).add(reference);
          }

          for (int i = 0; i < names.size(); i++) {
            addConstraint(names.get(i), "FOREIGN KEY", columns.get(i), references.get(i));
          }
        }

        // 3. UNIQUE constraints
        try (final ResultSet uq =
            dbmd.getIndexInfo(catalogName, dbSchemaName, tableName, true, false)) {
          Map<String, ArrayList<@Nullable String>> uniqueConstraints = new HashMap<>();
          while (uq.next()) {
            @Nullable String constraintName = uq.getString(6);
            @Nullable String columnName = uq.getString(9);
            int columnIndex = uq.getInt(8);

            if (constraintName == null || columnName == null) {
              throw new AdbcException(
                  JdbcDriverUtil.prefixExceptionMessage(
                      "JDBC driver returned null constraint/column name"),
                  null,
                  AdbcStatusCode.INTERNAL,
                  null,
                  0);
            }

            if (!uniqueConstraints.containsKey(constraintName)) {
              uniqueConstraints.put(constraintName, new ArrayList<>());
            }
            ArrayList<@Nullable String> uniqueColumns = uniqueConstraints.get(constraintName);
            while (uniqueColumns.size() < columnIndex) uniqueColumns.add(null);
            uniqueColumns.set(columnIndex - 1, columnName);
          }

          uniqueConstraints.forEach(
              (name, columns) -> {
                addConstraint(name, "UNIQUE", columns, Collections.emptyList());
              });
        }

        // TODO: how to get CHECK constraints?
        tableConstraintsWriter.endList();

        if (depth == AdbcConnection.GetObjectsDepth.TABLES) {
          tableColumns.setNull(rowIndex + tableCount);
        } else {
          int columnBaseIndex = tableColumns.startNewValue(rowIndex + tableCount);
          final int columnCount =
              buildColumns(columnBaseIndex, catalogName, dbSchemaName, tableName);
          tableColumns.endValue(rowIndex + tableCount, columnCount);
        }
        tableCount++;
      }
    }
    return tableCount;
  }

  private int buildColumns(int rowIndex, String catalogName, String dbSchemaName, String tableName)
      throws AdbcException, SQLException {
    int columnCount = 0;
    try (final ResultSet rs =
        dbmd.getColumns(catalogName, dbSchemaName, tableName, columnNamePattern)) {
      while (rs.next()) {
        final @Nullable String columnName = rs.getString(4);
        if (columnName == null) {
          throw new AdbcException(
              JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null column name"),
              null,
              AdbcStatusCode.INTERNAL,
              null,
              0);
        }
        final int ordinalPosition = rs.getInt(17);
        final @Nullable String remarks = rs.getString(12);
        final int xdbcDataType = rs.getInt(5);
        // TODO: other JDBC metadata

        columns.setIndexDefined(rowIndex + columnCount);
        columnNames.setSafe(rowIndex + columnCount, columnName.getBytes(StandardCharsets.UTF_8));
        columnOrdinalPositions.setSafe(rowIndex + columnCount, ordinalPosition);
        if (remarks != null) {
          columnRemarks.setSafe(rowIndex + columnCount, remarks.getBytes(StandardCharsets.UTF_8));
        }
        columnXdbcDataTypes.setSafe(rowIndex + columnCount, xdbcDataType);

        columnCount++;
      }
    }
    return columnCount;
  }

  private void addConstraint(
      @Nullable String constraintName,
      String constraintType,
      List<@Nullable String> constraintColumns,
      List<ReferencedColumn> referencedColumns) {
    tableConstraintsStructWriter.start();

    if (constraintName == null) {
      this.constraintNamesWriter.writeNull();
    } else {
      this.constraintNamesWriter.writeVarChar(constraintName);
    }
    this.constraintTypesWriter.writeVarChar(constraintType);

    constraintColumnNamesWriter.startList();
    for (final @Nullable String constraintColumn : constraintColumns) {
      VarCharWriter writer = constraintColumnNamesWriter.varChar();
      if (constraintColumn == null) {
        writer.writeNull();
      } else {
        writer.writeVarChar(constraintColumn);
      }
    }
    constraintColumnNamesWriter.endList();

    constraintColumnUsageWriter.startList();
    for (ReferencedColumn referencedColumn : referencedColumns) {
      constraintColumnUsageStructWriter.start();
      if (referencedColumn.catalog != null) {
        constraintColumnUsageFkCatalogsWriter.writeVarChar(referencedColumn.catalog);
      } else {
        constraintColumnUsageFkCatalogsWriter.writeNull();
      }
      if (referencedColumn.dbSchema != null) {
        constraintColumnUsageFkDbSchemasWriter.writeVarChar(referencedColumn.dbSchema);
      } else {
        constraintColumnUsageFkDbSchemasWriter.writeNull();
      }
      constraintColumnUsageFkTablesWriter.writeVarChar(referencedColumn.table);
      constraintColumnUsageFkColumnsWriter.writeVarChar(referencedColumn.column);
      constraintColumnUsageStructWriter.end();
    }
    constraintColumnUsageWriter.endList();

    tableConstraintsStructWriter.end();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(root);
  }

  /** Turn a SQL-style pattern (%, _) to a regex. */
  static String translatePattern(String filter) {
    StringBuilder builder = new StringBuilder(filter.length());
    builder.append("^");
    for (char c : filter.toCharArray()) {
      if (c == '%') {
        builder.append(".*");
      } else if (c == '_') {
        builder.append(".");
      } else {
        builder.append(Pattern.quote(String.valueOf(c)));
      }
    }
    builder.append("$");
    return builder.toString();
  }

  static class ReferencedColumn {
    @Nullable String catalog;
    @Nullable String dbSchema;
    String table;
    String column;

    public ReferencedColumn(
        @Nullable String catalog, @Nullable String dbSchema, String table, String column)
        throws AdbcException {
      this.catalog = catalog;
      this.dbSchema = dbSchema;
      if (table == null || column == null) {
        throw new AdbcException(
            JdbcDriverUtil.prefixExceptionMessage("JDBC driver returned null table/column name"),
            null,
            AdbcStatusCode.INTERNAL,
            null,
            0);
      }
      this.table = table;
      this.column = column;
    }
  }
}
