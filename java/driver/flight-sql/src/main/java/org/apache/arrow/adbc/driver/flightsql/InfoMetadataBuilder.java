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
package org.apache.arrow.adbc.driver.flightsql;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;

/** Helper class to track state needed to build up the info structure. */
final class InfoMetadataBuilder implements AutoCloseable {
  private static final byte STRING_VALUE_TYPE_ID = (byte) 0;
  private static final Map<Integer, Integer> ADBC_TO_FLIGHT_SQL_CODES = new HashMap<>();
  private static final Map<Integer, AddInfo> SUPPORTED_CODES = new HashMap<>();

  private final Collection<Integer> requestedCodes;
  private final FlightSqlClient client;
  private VectorSchemaRoot root;

  private final UInt4Vector infoCodes;
  private final DenseUnionVector infoValues;
  private final VarCharVector stringValues;

  @FunctionalInterface
  interface AddInfo {
    void accept(InfoMetadataBuilder builder, DenseUnionVector sqlInfo, int srcIndex, int dstIndex);
  }

  static {
    ADBC_TO_FLIGHT_SQL_CODES.put(
        AdbcInfoCode.VENDOR_NAME.getValue(), FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME.getNumber());
    ADBC_TO_FLIGHT_SQL_CODES.put(
        AdbcInfoCode.VENDOR_VERSION.getValue(),
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION.getNumber());

    SUPPORTED_CODES.put(
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME.getNumber(),
        (b, sqlInfo, srcIndex, dstIndex) -> {
          b.infoCodes.setSafe(dstIndex, AdbcInfoCode.VENDOR_NAME.getValue());
          b.setStringValue(dstIndex, sqlInfo.getVarCharVector(STRING_VALUE_TYPE_ID).get(srcIndex));
        });
    SUPPORTED_CODES.put(
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION.getNumber(),
        (b, sqlInfo, srcIndex, dstIndex) -> {
          b.infoCodes.setSafe(dstIndex, AdbcInfoCode.VENDOR_VERSION.getValue());
          b.setStringValue(dstIndex, sqlInfo.getVarCharVector(STRING_VALUE_TYPE_ID).get(srcIndex));
        });
  }

  InfoMetadataBuilder(BufferAllocator allocator, FlightSqlClient client, int[] infoCodes) {
    if (infoCodes == null) {
      this.requestedCodes = new ArrayList<>(SUPPORTED_CODES.keySet());
      this.requestedCodes.add(AdbcInfoCode.DRIVER_NAME.getValue());
      this.requestedCodes.add(AdbcInfoCode.DRIVER_VERSION.getValue());
    } else {
      this.requestedCodes = IntStream.of(infoCodes).boxed().collect(Collectors.toList());
    }
    this.client = client;
    this.root = VectorSchemaRoot.create(StandardSchemas.GET_INFO_SCHEMA, allocator);
    this.infoCodes = (UInt4Vector) root.getVector(0);
    this.infoValues = (DenseUnionVector) root.getVector(1);
    this.stringValues = this.infoValues.getVarCharVector((byte) 0);
  }

  void setStringValue(int index, byte[] value) {
    infoValues.setValueCount(index + 1);
    infoValues.setTypeId(index, STRING_VALUE_TYPE_ID);
    stringValues.setSafe(index, value);
    infoValues
        .getOffsetBuffer()
        .setInt((long) index * DenseUnionVector.OFFSET_WIDTH, stringValues.getLastSet());
  }

  VectorSchemaRoot build() throws AdbcException {
    // XXX: rather hacky, we need a better way to do this
    int dstIndex = 0;

    List<Integer> translatedCodes = new ArrayList<>();
    for (int code : requestedCodes) {
      Integer translatedCode = ADBC_TO_FLIGHT_SQL_CODES.get(code);
      if (translatedCode != null) {
        translatedCodes.add(translatedCode);
      } else if (code == AdbcInfoCode.DRIVER_NAME.getValue()) {
        infoCodes.setSafe(dstIndex, code);
        setStringValue(dstIndex++, "ADBC Flight SQL Driver".getBytes(StandardCharsets.UTF_8));
      } else if (code == AdbcInfoCode.DRIVER_VERSION.getValue()) {
        infoCodes.setSafe(dstIndex, code);
        // TODO: actual version
        setStringValue(dstIndex++, "0.0.1".getBytes(StandardCharsets.UTF_8));
      }
    }
    final FlightInfo info = client.getSqlInfo(translatedCodes);

    for (final FlightEndpoint endpoint : info.getEndpoints()) {
      // TODO: this should account for locations property
      try (final FlightStream stream = client.getStream(endpoint.getTicket())) {
        final VectorSchemaRoot root = stream.getRoot();
        final UInt4Vector sqlCode = (UInt4Vector) root.getVector(0);
        final DenseUnionVector sqlInfo = (DenseUnionVector) root.getVector(1);
        for (int srcIndex = 0; srcIndex < root.getRowCount(); srcIndex++) {
          final AddInfo addInfo = SUPPORTED_CODES.get(sqlCode.get(srcIndex));
          if (addInfo != null) {
            addInfo.accept(this, sqlInfo, srcIndex, dstIndex++);
          }
        }
      } catch (FlightRuntimeException e) {
        throw FlightSqlDriverUtil.fromFlightException(e);
      } catch (Exception e) {
        throw AdbcException.io("[Flight SQL] " + e.getMessage()).withCause(e);
      }
    }

    root.setRowCount(dstIndex);
    VectorSchemaRoot result = root;
    root = null;
    return result;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(root);
  }
}
