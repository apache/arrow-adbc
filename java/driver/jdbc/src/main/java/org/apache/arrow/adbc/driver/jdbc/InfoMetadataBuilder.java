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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper class to track state needed to build up the info structure. */
final class InfoMetadataBuilder implements AutoCloseable {
  private static final byte STRING_VALUE_TYPE_ID = (byte) 0;
  private static final byte BIGINT_VALUE_TYPE_ID = (byte) 2;
  private static final Map<Integer, AddInfo> SUPPORTED_CODES = new HashMap<>();
  private final BufferAllocator allocator;
  private final Collection<Integer> requestedCodes;
  private final DatabaseMetaData dbmd;
  private VectorSchemaRoot root;

  final UInt4Vector infoCodes;
  final DenseUnionVector infoValues;
  final VarCharVector stringValues;
  final BigIntVector bigIntValues;

  @FunctionalInterface
  interface AddInfo {
    void accept(InfoMetadataBuilder builder, int rowIndex) throws SQLException;
  }

  static {
    SUPPORTED_CODES.put(
        AdbcInfoCode.VENDOR_NAME.getValue(),
        (b, idx) -> {
          b.setStringValue(idx, b.dbmd.getDatabaseProductName());
        });
    SUPPORTED_CODES.put(
        AdbcInfoCode.VENDOR_VERSION.getValue(),
        (b, idx) -> {
          b.setStringValue(idx, b.dbmd.getDatabaseProductVersion());
        });
    SUPPORTED_CODES.put(
        AdbcInfoCode.DRIVER_NAME.getValue(),
        (b, idx) -> {
          final String driverName = "ADBC JDBC Driver (" + b.dbmd.getDriverName() + ")";
          b.setStringValue(idx, driverName);
        });
    SUPPORTED_CODES.put(
        AdbcInfoCode.DRIVER_VERSION.getValue(),
        (b, idx) -> {
          final String driverVersion = b.dbmd.getDriverVersion() + " (ADBC Driver Version 0.0.1)";
          b.setStringValue(idx, driverVersion);
        });
    SUPPORTED_CODES.put(
        AdbcInfoCode.DRIVER_ADBC_VERSION.getValue(),
        (b, idx) -> {
          b.setBigIntValue(idx, AdbcDriver.ADBC_VERSION_1_1_0);
        });
  }

  InfoMetadataBuilder(BufferAllocator allocator, Connection connection, int @Nullable [] infoCodes)
      throws SQLException {
    this.allocator = allocator;
    if (infoCodes == null) {
      this.requestedCodes = new ArrayList<>(SUPPORTED_CODES.keySet());
    } else {
      this.requestedCodes = IntStream.of(infoCodes).boxed().collect(Collectors.toList());
    }
    this.root = VectorSchemaRoot.create(StandardSchemas.GET_INFO_SCHEMA, allocator);
    this.dbmd = connection.getMetaData();
    this.infoCodes = (UInt4Vector) root.getVector(0);
    this.infoValues = (DenseUnionVector) root.getVector(1);
    this.stringValues = this.infoValues.getVarCharVector(STRING_VALUE_TYPE_ID);
    this.bigIntValues = this.infoValues.getBigIntVector(BIGINT_VALUE_TYPE_ID);
  }

  void setBigIntValue(int index, long value) {
    infoValues.setValueCount(index + 1);
    infoValues.setTypeId(index, BIGINT_VALUE_TYPE_ID);
    bigIntValues.setSafe(index, value);
    infoValues
        .getOffsetBuffer()
        .setInt((long) index * DenseUnionVector.OFFSET_WIDTH, bigIntValues.getValueCount());
    bigIntValues.setValueCount(bigIntValues.getValueCount() + 1);
  }

  void setStringValue(int index, final String value) {
    infoValues.setValueCount(index + 1);
    infoValues.setTypeId(index, STRING_VALUE_TYPE_ID);
    stringValues.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
    infoValues
        .getOffsetBuffer()
        .setInt((long) index * DenseUnionVector.OFFSET_WIDTH, stringValues.getLastSet());
  }

  VectorSchemaRoot build() throws SQLException {
    int rowIndex = 0;
    for (final Integer code : requestedCodes) {
      final AddInfo metadata = SUPPORTED_CODES.get(code);
      if (metadata == null) {
        continue;
      }
      infoCodes.setSafe(rowIndex, code);
      metadata.accept(this, rowIndex++);
    }
    root.setRowCount(rowIndex);
    VectorSchemaRoot result = root;
    try {
      root = VectorSchemaRoot.create(StandardSchemas.GET_INFO_SCHEMA, allocator);
    } catch (RuntimeException e) {
      result.close();
      throw e;
    }
    return result;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(root);
  }
}
