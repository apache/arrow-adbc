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
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An ArrowReader that wraps a JDBC ResultSet. */
public class JdbcArrowReader extends ArrowReader {
  private final ArrowVectorIterator delegate;
  private final Schema schema;
  private long bytesRead;

  // ensureInitialized() call isn't annotated right
  @SuppressWarnings({"under.initialization", "method.invocation"})
  JdbcArrowReader(BufferAllocator allocator, ResultSet resultSet, @Nullable Schema overrideSchema)
      throws AdbcException {
    super(allocator);
    final JdbcToArrowConfig config = makeJdbcConfig(allocator);
    try {
      this.delegate = JdbcToArrow.sqlToArrowVectorIterator(resultSet, config);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    } catch (IOException e) {
      throw new AdbcException(
          JdbcDriverUtil.prefixExceptionMessage(e.getMessage()), e, AdbcStatusCode.IO, null, -1);
    }
    if (overrideSchema != null) {
      this.schema = overrideSchema;
    } else {
      try {
        this.schema = JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), config);
      } catch (SQLException e) {
        throw JdbcDriverUtil.fromSqlException("Failed to convert JDBC schema to Arrow schema:", e);
      }
    }
    this.bytesRead = 0;

    try {
      this.ensureInitialized();
    } catch (IOException e) {
      throw new AdbcException(
          JdbcDriverUtil.prefixExceptionMessage(e.getMessage()), e, AdbcStatusCode.IO, null, 0);
    }
  }

  static JdbcToArrowConfig makeJdbcConfig(BufferAllocator allocator) {
    return new JdbcToArrowConfigBuilder()
        .setAllocator(allocator)
        .setCalendar(JdbcToArrowUtils.getUtcCalendar())
        .setTargetBatchSize(1024)
        .build();
  }

  @Override
  public boolean loadNextBatch() {
    if (!delegate.hasNext()) return false;
    try (final VectorSchemaRoot root = delegate.next()) {
      final VectorUnloader unloader = new VectorUnloader(root);
      try (final ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
        bytesRead += recordBatch.computeBodyLength();
        loadRecordBatch(recordBatch);
      }
    }
    return true;
  }

  @Override
  public long bytesRead() {
    return bytesRead;
  }

  @Override
  protected void closeReadSource() {
    delegate.close();
  }

  @Override
  protected Schema readSchema() {
    return schema;
  }
}
