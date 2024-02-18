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

import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/** Helper class to track state needed to build up the info structure. */
final class GetInfoMetadataReader extends BaseFlightReader {
  private static final byte STRING_VALUE_TYPE_ID = (byte) 0;
  private static final Map<Integer, Integer> ADBC_TO_FLIGHT_SQL_CODES = new HashMap<>();
  private static final Map<Integer, AddInfo> SUPPORTED_CODES = new HashMap<>();
  private static final byte[] DRIVER_NAME =
      "ADBC Flight SQL Driver".getBytes(StandardCharsets.UTF_8);

  private final BufferAllocator allocator;
  private final Collection<Integer> requestedCodes;
  private @Nullable UInt4Vector infoCodes = null;
  private @Nullable DenseUnionVector infoValues = null;
  private @Nullable VarCharVector stringValues = null;
  private boolean hasInMemoryDataBeenWritten = false;
  private final boolean hasInMemoryData;
  private final boolean hasSupportedCodes;
  private boolean hasRequestBeenIssued = false;

  @FunctionalInterface
  interface AddInfo {
    void accept(
        GetInfoMetadataReader builder, DenseUnionVector sqlInfo, int srcIndex, int dstIndex);
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
          if (b.infoCodes == null) {
            throw new IllegalStateException();
          }
          b.infoCodes.setSafe(dstIndex, AdbcInfoCode.VENDOR_NAME.getValue());
          b.setStringValue(dstIndex, sqlInfo.getVarCharVector(STRING_VALUE_TYPE_ID).get(srcIndex));
        });
    SUPPORTED_CODES.put(
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION.getNumber(),
        (b, sqlInfo, srcIndex, dstIndex) -> {
          if (b.infoCodes == null) {
            throw new IllegalStateException();
          }
          b.infoCodes.setSafe(dstIndex, AdbcInfoCode.VENDOR_VERSION.getValue());
          b.setStringValue(dstIndex, sqlInfo.getVarCharVector(STRING_VALUE_TYPE_ID).get(srcIndex));
        });
  }

  static GetInfoMetadataReader CreateGetInfoMetadataReader(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      int @Nullable [] infoCodes) {
    LinkedHashSet<Integer> requestedCodes;
    if (infoCodes == null) {
      requestedCodes = new LinkedHashSet<>(SUPPORTED_CODES.keySet());
      requestedCodes.add(AdbcInfoCode.DRIVER_NAME.getValue());
      requestedCodes.add(AdbcInfoCode.DRIVER_VERSION.getValue());
    } else {
      requestedCodes =
          IntStream.of(infoCodes)
              .sorted()
              .boxed()
              .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    return new GetInfoMetadataReader(allocator, client, clientCache, requestedCodes);
  }

  GetInfoMetadataReader(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      Collection<Integer> requestedCodes) {
    super(allocator, client, clientCache, () -> issueGetSqlInfoRequest(client, requestedCodes));
    this.requestedCodes = requestedCodes;
    this.allocator = allocator;
    this.hasInMemoryData =
        requestedCodes.contains(AdbcInfoCode.DRIVER_NAME.getValue())
            || requestedCodes.contains(AdbcInfoCode.DRIVER_VERSION.getValue());
    this.hasSupportedCodes = requestedCodes.stream().anyMatch(SUPPORTED_CODES::containsKey);
  }

  @SuppressWarnings("dereference.of.nullable")
  // Framework is treating vector calls as having potential side-effects that later the nullity of
  // fields.
  @Pure
  void setStringValue(int index, byte[] value) {
    infoValues.setValueCount(index + 1);
    infoValues.setTypeId(index, STRING_VALUE_TYPE_ID);
    stringValues.setSafe(index, value);
    infoValues
        .getOffsetBuffer()
        .setInt((long) index * DenseUnionVector.OFFSET_WIDTH, stringValues.getLastSet());
  }

  @SuppressWarnings("dereference.of.nullable")
  // Checker framework is considering Arrow methods such as getVarCharVectors() as impure and
  // possibly altering
  // the state of fields such as infoCodes.
  @Override
  public boolean loadNextBatch() throws IOException {
    if (hasInMemoryData && !hasInMemoryDataBeenWritten) {
      // Write in-memory constant entries into the first root. Subsequent roots
      // only contain data sent from FlightSQL RPC calls.
      // XXX: rather hacky, we need a better way to do this
      hasInMemoryDataBeenWritten = true;
      int dstIndex = 0;
      try (VectorSchemaRoot root = VectorSchemaRoot.create(readSchema(), allocator)) {
        root.allocateNew();
        this.infoCodes = (UInt4Vector) root.getVector(0);
        this.infoValues = (DenseUnionVector) root.getVector(1);
        this.stringValues = this.infoValues.getVarCharVector((byte) 0);

        if (requestedCodes.contains(AdbcInfoCode.DRIVER_NAME.getValue())) {
          infoCodes.setSafe(dstIndex, AdbcInfoCode.DRIVER_NAME.getValue());
          setStringValue(dstIndex++, DRIVER_NAME);
        }

        if (requestedCodes.contains(AdbcInfoCode.DRIVER_VERSION.getValue())) {
          infoCodes.setSafe(dstIndex, AdbcInfoCode.DRIVER_VERSION.getValue());
          // TODO: actual version
          setStringValue(dstIndex++, "0.0.1".getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(dstIndex);
        loadRoot(root);
        return true;
      }
    }

    if (hasSupportedCodes) {
      if (!hasRequestBeenIssued) {
        hasRequestBeenIssued = true;
        try {
          populateEndpointData();
        } catch (AdbcException e) {
          throw new IOException(e);
        }
      }
      return super.loadNextBatch();
    }

    return false;
  }

  @Override
  protected Schema readSchema() {
    return StandardSchemas.GET_INFO_SCHEMA;
  }

  @Override
  protected void processRootFromStream(VectorSchemaRoot root) {
    try (VectorSchemaRoot tmpRoot = VectorSchemaRoot.create(readSchema(), allocator)) {
      root.allocateNew();
      this.infoCodes = (UInt4Vector) tmpRoot.getVector(0);
      this.infoValues = (DenseUnionVector) tmpRoot.getVector(1);
      this.stringValues = this.infoValues.getVarCharVector((byte) 0);

      int dstIndex = 0;
      final UInt4Vector sqlCode = (UInt4Vector) root.getVector(0);
      final DenseUnionVector sqlInfo = (DenseUnionVector) root.getVector(1);
      for (int srcIndex = 0; srcIndex < root.getRowCount(); srcIndex++) {
        final AddInfo addInfo = SUPPORTED_CODES.get(sqlCode.get(srcIndex));
        if (addInfo != null) {
          addInfo.accept(this, sqlInfo, srcIndex, dstIndex++);
        }
      }

      tmpRoot.setRowCount(dstIndex);
      loadRoot(tmpRoot);
    }
  }

  private static List<FlightEndpoint> issueGetSqlInfoRequest(
      FlightSqlClientWithCallOptions client, Collection<Integer> requestedCodes) {
    List<Integer> translatedCodes = new ArrayList<>();
    for (int code : requestedCodes) {
      Integer translatedCode = ADBC_TO_FLIGHT_SQL_CODES.get(code);
      if (translatedCode != null) {
        translatedCodes.add(translatedCode);
      }
    }

    return client.getSqlInfo(translatedCodes).getEndpoints();
  }
}
