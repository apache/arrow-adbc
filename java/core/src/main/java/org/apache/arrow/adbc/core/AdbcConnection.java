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

import java.nio.ByteBuffer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A connection to a {@link AdbcDatabase}.
 *
 * <p>Connections are not required to be thread-safe, but they can be used from multiple threads so
 * long as clients take care to serialize accesses to a connection.
 */
public interface AdbcConnection extends AdbcCloseable, AdbcOptions {
  /**
   * Cancel execution of a query.
   *
   * <p>This can be used to interrupt execution of a method like {@link #getObjects(GetObjectsDepth,
   * String, String, String, String[], String)}}.
   *
   * <p>This method must be thread-safe (other method are not necessarily thread-safe).
   *
   * @since ADBC API revision 1.1.0
   */
  default void cancel() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support cancel");
  }

  /** Commit the pending transaction. */
  default void commit() throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support transactions");
  }

  /** Create a new statement that can be executed. */
  AdbcStatement createStatement() throws AdbcException;

  /**
   * Create a new statement to bulk insert a {@link VectorSchemaRoot} into a table.
   *
   * <p>Bind data to the statement, then call {@link AdbcStatement#executeUpdate()}. See {@link
   * BulkIngestMode} for description of behavior around creating tables.
   */
  default AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    throw AdbcException.notImplemented(
        "Connection does not support bulkIngest(String, BulkIngestMode)");
  }

  /**
   * Create a result set from a serialized PartitionDescriptor.
   *
   * @param descriptor The descriptor to load ({@link PartitionDescriptor#getDescriptor()}.
   * @return A statement that can be immediately executed.
   * @see AdbcStatement.PartitionResult
   */
  default ArrowReader readPartition(ByteBuffer descriptor) throws AdbcException {
    throw AdbcException.notImplemented(
        "Connection does not support deserializePartitionDescriptor(ByteBuffer)");
  }

  /**
   * Get metadata about the driver/database.
   *
   * @param infoCodes The metadata items to fetch.
   */
  ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException;

  /**
   * Get metadata about the driver/database.
   *
   * @param infoCodes The metadata items to fetch.
   * @return A statement that can be immediately executed.
   */
  default ArrowReader getInfo(AdbcInfoCode[] infoCodes) throws AdbcException {
    int[] codes = new int[infoCodes.length];
    for (int i = 0; i < infoCodes.length; i++) {
      codes[i] = infoCodes[i].getValue();
    }
    return getInfo(codes);
  }

  /**
   * Get metadata about the driver/database.
   *
   * @return A statement that can be immediately executed.
   */
  default ArrowReader getInfo() throws AdbcException {
    return getInfo((int[]) null);
  }

  /**
   * Get a hierarchical view of all catalogs, database schemas, tables, and columns.
   *
   * <p>The result is an Arrow dataset with the following schema:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             </tr>
   *   <tr><td>catalog_name</td>            <td>utf8</td>                   </tr>
   *   <tr><td>catalog_db_schemas</td>      <td>list[DB_SCHEMA_SCHEMA]</td> </tr>
   *   <caption>The definition of the GetObjects result schema.</caption>
   * </table>
   *
   * <p>DB_SCHEMA_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             </tr>
   *   <tr><td>db_schema_name</td>          <td>utf8</td>                   </tr>
   *   <tr><td>db_schema_tables</td>        <td>list[TABLE_SCHEMA]</td>     </tr>
   *   <caption>The definition of DB_SCHEMA_SCHEMA.</caption>
   * </table>
   *
   * <p>TABLE_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             </tr>
   *   <tr><td>table_name</td>              <td>utf8 not null</td>          </tr>
   *   <tr><td>table_type</td>              <td>utf8 not null</td>          </tr>
   *   <tr><td>table_columns</td>           <td>list[COLUMN_SCHEMA]</td>    </tr>
   *   <tr><td>table_constraints</td>       <td>list[CONSTRAINT_SCHEMA]</td></tr>
   *   <caption>The definition of TABLE_SCHEMA.</caption>
   * </table>
   *
   * <p>COLUMN_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             <th>Comments</th></tr>
   *   <tr><td>column_name</td>             <td>utf8 not null</td>          <td></td></tr>
   *   <tr><td>ordinal_position</td>        <td>int32</td>                  <td>(1)</td></tr>
   *   <tr><td>remarks</td>                 <td>utf8</td>                   <td>(2)</td></tr>
   *   <tr><td>xdbc_data_type</td>          <td>int16</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_type_name</td>          <td>utf8 </td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_column_size</td>        <td>int32</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_decimal_digits</td>     <td>int16</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_num_prec_radix</td>     <td>int16</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_nullable</td>           <td>int16</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_column_def</td>         <td>utf8</td>                   <td>(3)</td></tr>
   *   <tr><td>xdbc_sql_data_type</td>      <td>int16</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_datetime_sub</td>       <td>int16</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_char_octet_length</td>  <td>int32</td>                  <td>(3)</td></tr>
   *   <tr><td>xdbc_is_nullable</td>        <td>utf8</td>                   <td>(3)</td></tr>
   *   <tr><td>xdbc_scope_catalog</td>      <td>utf8</td>                   <td>(3)</td></tr>
   *   <tr><td>xdbc_scope_schema</td>       <td>utf8</td>                   <td>(3)</td></tr>
   *   <tr><td>xdbc_scope_table</td>        <td>utf8</td>                   <td>(3)</td></tr>
   *   <tr><td>xdbc_is_autoincrement</td>   <td>bool</td>                   <td>(3)</td></tr>
   *   <tr><td>xdbc_is_generatedcolumn</td> <td>bool</td>                   <td>(3)</td></tr>
   *   <caption>The definition of COLUMN_SCHEMA.</caption>
   * </table>
   *
   * <p>Notes:
   *
   * <ol>
   *   <li>The column's ordinal position in the table (starting from 1).
   *   <li>Database-specific description of the column.
   *   <li>Optional value. Should be null if not supported by the driver. xdbc_ values are meant to
   *       provide JDBC/ODBC-compatible metadata in an agnostic manner.
   * </ol>
   *
   * <p>CONSTRAINT_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             <th>Comments</th></tr>
   *   <tr><td>constraint_name</td>         <td>utf8 not null</td>          <td></td></tr>
   *   <tr><td>constraint_type</td>         <td>utf8 not null</td>          <td>(1)</td></tr>
   *   <tr><td>constraint_column_names</td> <td>list[utf8] not null</td>    <td>(2)</td></tr>
   *   <tr><td>constraint_column_usage</td> <td>list[USAGE_SCHEMA]</td>     <td>(3)</td></tr>
   *   <caption>The definition of CONSTRAINT_SCHEMA.</caption>
   * </table>
   *
   * <ol>
   *   <li>One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
   *   <li>The columns on the current table that are constrained, in order.
   *   <li>For FOREIGN KEY only, the referenced table and columns.
   * </ol>
   *
   * <p>USAGE_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             </tr>
   *   <tr><td>fk_catalog</td>              <td>utf8</td>                   </tr>
   *   <tr><td>fk_db_schema</td>            <td>utf8</td>                   </tr>
   *   <tr><td>fk_table</td>                <td>utf8 not null</td>          </tr>
   *   <tr><td>fk_column_name</td>          <td>utf8 not null</td>          </tr>
   *   <caption>The definition of USAGE_SCHEMA.</caption>
   * </table>
   *
   * @param depth The level of nesting to display. If ALL, display all levels (up through columns).
   *     If CATALOGS, display only catalogs (i.e. catalog_schemas will be null), and so on. May be a
   *     search pattern (see class documentation).
   * @param catalogPattern Only show tables in the given catalog. If null, do not filter by catalog.
   *     If an empty string, only show tables without a catalog. May be a search pattern (see class
   *     documentation).
   * @param dbSchemaPattern Only show tables in the given database schema. If null, do not filter by
   *     database schema. If an empty string, only show tables without a database schema. May be a
   *     search pattern (see class documentation).
   * @param tableNamePattern Only show tables with the given name. If an empty string, only show
   *     tables without a catalog. May be a search pattern (see class documentation).
   * @param tableTypes Only show tables matching one of the given table types. If null, show tables
   *     of any type. Valid table types can be fetched from {@link #getTableTypes()}.
   * @param columnNamePattern Only show columns with the given name. If null, do not filter by name.
   *     May be a search pattern (see class documentation).
   */
  default ArrowReader getObjects(
      GetObjectsDepth depth,
      String catalogPattern,
      String dbSchemaPattern,
      String tableNamePattern,
      String[] tableTypes,
      String columnNamePattern)
      throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support getObjects()");
  }

  /**
   * The level of nesting to retrieve for {@link #getObjects(GetObjectsDepth, String, String,
   * String, String[], String)}.
   */
  enum GetObjectsDepth {
    /** Display ALL objects (catalog, database schemas, tables, and columns). */
    ALL,
    /** Display only catalogs. */
    CATALOGS,
    /** Display catalogs and database schemas. */
    DB_SCHEMAS,
    /** Display catalogs, database schemas, and tables. */
    TABLES,
  }

  /**
   * Get statistics about the data distribution of table(s).
   *
   * <p>The result is an Arrow dataset with the following schema:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>                      </tr>
   *   <tr><td>catalog_name</td>            <td>utf8</td>                            </tr>
   *   <tr><td>catalog_db_schemas</td>      <td>list[DB_SCHEMA_SCHEMA] not null</td> </tr>
   *   <caption>The definition of the GetStatistics result schema.</caption>
   * </table>
   *
   * <p>DB_SCHEMA_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>                      </tr>
   *   <tr><td>db_schema_name</td>          <td>utf8</td>                            </tr>
   *   <tr><td>db_schema_statistics</td>    <td>list[STATISTICS_SCHEMA] not null</td></tr>
   *   <caption>The definition of DB_SCHEMA_SCHEMA.</caption>
   * </table>
   *
   * <p>STATISTICS_SCHEMA is a Struct with fields:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             <th>Comments</th></tr>
   *   <tr><td>table_name</td>              <td>utf8 not null</td>          <td></td></tr>
   *   <tr><td>column_name</td>             <td>utf8</td>                   <td>(1)</td></tr>
   *   <tr><td>statistic_key</td>           <td>int16 not null</td>         <td>(2)</td></tr>
   *   <tr><td>statistic_value</td>         <td>VALUE_SCHEMA not null</td>  <td></td></tr>
   *   <tr><td>statistic_is_approximate</td><td>bool not null</td>          <td>(3)</td></tr>
   *   <caption>The definition of STATISTICS_SCHEMA.</caption>
   * </table>
   *
   * <ol>
   *   <li>If null, then the statistic applies to the entire table.
   *   <li>A dictionary-encoded statistic name (although we do not use the Arrow dictionary type).
   *       Values in [0, 1024) are reserved for ADBC. Other values are for implementation-specific
   *       statistics. For the definitions of predefined statistic types, see {@link
   *       StandardStatistics}. To get driver-specific statistic names, use {@link
   *       #getStatisticNames()}.
   *   <li>If true, then the value is approximate or best-effort.
   * </ol>
   *
   * <p>VALUE_SCHEMA is a dense union with members:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>             </tr>
   *   <tr><td>int64</td>                   <td>int64</td>                  </tr>
   *   <tr><td>uint64</td>                  <td>uint64</td>                 </tr>
   *   <tr><td>float64</td>                 <td>float64</td>                </tr>
   *   <tr><td>binary</td>                  <td>binary</td>                 </tr>
   *   <caption>The definition of VALUE_SCHEMA.</caption>
   * </table>
   *
   * @param catalogPattern Only show tables in the given catalog. If null, do not filter by catalog.
   *     If an empty string, only show tables without a catalog. May be a search pattern (see class
   *     documentation).
   * @param dbSchemaPattern Only show tables in the given database schema. If null, do not filter by
   *     database schema. If an empty string, only show tables without a database schema. May be a
   *     search pattern (see class documentation).
   * @param tableNamePattern Only show tables with the given name. If an empty string, only show
   *     tables without a catalog. May be a search pattern (see class documentation).
   * @param approximate If false, request exact values of statistics, else allow for best-effort,
   *     approximate, or cached values. The database may return approximate values regardless, as
   *     indicated in the result. Requesting exact values may be expensive or unsupported.
   */
  default ArrowReader getStatistics(
      String catalogPattern, String dbSchemaPattern, String tableNamePattern, boolean approximate)
      throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support getStatistics()");
  }

  /**
   * Get the names of additional statistics defined by this driver.
   *
   * <p>The result is an Arrow dataset with the following schema:
   *
   * <table border="1">
   *   <tr><th>Field Name</th>              <th>Field Type</th>    </tr>
   *   <tr><td>statistic_name</td>          <td>utf8 not null</td> </tr>
   *   <tr><td>statistic_key</td>           <td>int16 not null</td></tr>
   *   <caption>The definition of the GetStatistics result schema.</caption>
   * </table>
   */
  default ArrowReader getStatisticNames() throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support getStatisticNames()");
  }

  /**
   * Get the Arrow schema of a database table.
   *
   * @param catalog The catalog of the table (or null).
   * @param dbSchema The database schema of the table (or null).
   * @param tableName The table name.
   * @return The table schema.
   */
  default Schema getTableSchema(String catalog, String dbSchema, String tableName)
      throws AdbcException {
    throw AdbcException.notImplemented(
        "Connection does not support getTableSchema(String, String, String)");
  }

  /**
   * Get a list of table types supported by the database.
   *
   * <p>The result is an Arrow dataset with the following schema:
   *
   * <table border="1">
   *   <tr>
   *     <th>Field Name</th>
   *     <th>Field Type</th>
   *   </tr>
   *   <tr>
   *     <td>table_type</td>
   *     <td>utf8 not null</td>
   *   </tr>
   *   <caption>The definition of the result schema.</caption>
   * </table>
   */
  default ArrowReader getTableTypes() throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support getTableTypes()");
  }

  /**
   * Rollback the pending transaction.
   *
   * @throws AdbcException if a database error occurs
   */
  default void rollback() throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support transactions");
  }

  /**
   * Get the autocommit state.
   *
   * <p>Connections start in autocommit mode by default.
   */
  default boolean getAutoCommit() throws AdbcException {
    return true;
  }

  /** Toggle whether autocommit is enabled. */
  default void setAutoCommit(boolean enableAutoCommit) throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support transactions");
  }

  /**
   * Get the current catalog.
   *
   * @since ADBC API revision 1.1.0
   */
  default String getCurrentCatalog() throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support current catalog");
  }

  /**
   * Set the current catalog.
   *
   * @since ADBC API revision 1.1.0
   */
  default void setCurrentCatalog(String catalog) throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support current catalog");
  }

  /**
   * Get the current schema.
   *
   * @since ADBC API revision 1.1.0
   */
  default String getCurrentDbSchema() throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support current catalog");
  }

  /**
   * Set the current schema.
   *
   * @since ADBC API revision 1.1.0
   */
  default void setCurrentDbSchema(String dbSchema) throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support current catalog");
  }

  /**
   * Get whether the connection is read-only.
   *
   * <p>Connections are not read-only by default.
   */
  default boolean getReadOnly() throws AdbcException {
    return false;
  }

  /** Toggle whether the connection is read-only. */
  default void setReadOnly(boolean isReadOnly) throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support read-only mode");
  }

  /** Get the isolation level used by transactions. */
  default IsolationLevel getIsolationLevel() throws AdbcException {
    return IsolationLevel.DEFAULT;
  }

  /** Change the isolation level used by transactions. */
  default void setIsolationLevel(IsolationLevel level) throws AdbcException {
    throw AdbcException.notImplemented("Connection does not support setting isolation level");
  }
}
