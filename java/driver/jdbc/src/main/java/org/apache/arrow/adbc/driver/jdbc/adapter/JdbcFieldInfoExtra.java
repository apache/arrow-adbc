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

package org.apache.arrow.adbc.driver.jdbc.adapter;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Information about a column from JDBC for inferring column type.
 *
 * <p>Extension of the upstream {@link JdbcFieldInfo} which lacks necessary fields for certain
 * databases.
 */
public final class JdbcFieldInfoExtra {
  final JdbcFieldInfo info;
  final String typeName;
  final int numPrecRadix;
  final @Nullable String remarks;
  final @Nullable String columnDef;
  final int sqlDataType;
  final int sqlDatetimeSub;
  final int charOctetLength;
  final int ordinalPosition;

  /**
   * Create a JdbcFieldInfoExtra from the result of a {@link
   * java.sql.DatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * @param rs The result set.
   */
  public JdbcFieldInfoExtra(ResultSet rs) throws SQLException {
    final int dataType = rs.getInt("DATA_TYPE");
    final @Nullable String maybeTypeName = rs.getString("TYPE_NAME");
    if (maybeTypeName == null) {
      throw new RuntimeException("Field " + "TYPE_NAME" + " was null");
    }
    this.typeName = maybeTypeName;
    final int columnSize = rs.getInt("COLUMN_SIZE");
    final int decimalDigits = rs.getInt("DECIMAL_DIGITS");
    this.numPrecRadix = rs.getInt("NUM_PREC_RADIX");
    final int nullable = rs.getInt("NULLABLE");
    this.remarks = rs.getString("REMARKS");
    this.columnDef = rs.getString("COLUMN_DEF");
    this.sqlDataType = rs.getInt("SQL_DATA_TYPE");
    this.sqlDatetimeSub = rs.getInt("SQL_DATETIME_SUB");
    this.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
    this.ordinalPosition = rs.getInt("ORDINAL_POSITION");

    this.info = new JdbcFieldInfo(dataType, nullable, columnSize, decimalDigits);
  }

  /** Get the base Arrow version. */
  public JdbcFieldInfo getFieldInfo() {
    return info;
  }

  /** The {@link java.sql.Types} type. */
  public int getJdbcType() {
    return info.getJdbcType();
  }

  public String getTypeName() {
    return typeName;
  }

  public int getNumPrecRadix() {
    return numPrecRadix;
  }

  public @Nullable String getRemarks() {
    return remarks;
  }

  public @Nullable String getColumnDef() {
    return columnDef;
  }

  public int getSqlDataType() {
    return sqlDataType;
  }

  public int getSqlDatetimeSub() {
    return sqlDatetimeSub;
  }

  public int getCharOctetLength() {
    return charOctetLength;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  /** The nullability. */
  public int isNullable() {
    return info.isNullable();
  }

  /**
   * The numeric precision, for {@link java.sql.Types#NUMERIC} and {@link java.sql.Types#DECIMAL}
   * types.
   */
  public int getPrecision() {
    return info.getPrecision();
  }

  /**
   * The numeric scale, for {@link java.sql.Types#NUMERIC} and {@link java.sql.Types#DECIMAL} types.
   */
  public int getScale() {
    return info.getScale();
  }

  /** The column index for query column. */
  public int getColumn() {
    return info.getColumn();
  }

  @Override
  public String toString() {
    return "JdbcFieldInfoExtra{"
        + "dataType="
        + info.getJdbcType()
        + ", typeName='"
        + typeName
        + '\''
        + ", columnSize="
        + info.getPrecision()
        + ", decimalDigits="
        + info.getScale()
        + ", numPrecRadix="
        + numPrecRadix
        + ", nullable="
        + info.isNullable()
        + ", remarks='"
        + remarks
        + '\''
        + ", columnDef='"
        + columnDef
        + '\''
        + ", sqlDataType="
        + sqlDataType
        + ", sqlDatetimeSub="
        + sqlDatetimeSub
        + ", charOctetLength="
        + charOctetLength
        + ", ordinalPosition="
        + ordinalPosition
        + '}';
  }
}
