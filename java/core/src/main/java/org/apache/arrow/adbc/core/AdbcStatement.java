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
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

public interface AdbcStatement extends AutoCloseable {
  /**
   * The name of the target table for a bulk insert.
   *
   * @see #setOption(String, String)
   */
  String INGEST_OPTION_TARGET_TABLE = "adbc.ingest.target_table";

  /** Set a generic query option. */
  default void setOption(String key, String value) {
    throw new UnsupportedOperationException("Unsupported option " + key);
  }

  /**
   * Set a SQL query to be executed on this statement.
   *
   * @param query The SQL query.
   */
  default void setSqlQuery(String query) {
    throw new UnsupportedOperationException("Statement does not support SQL queries");
  }

  /**
   * Set a Substrait plan to be executed on this statement.
   *
   * @param plan The serialized Substrait plan.
   */
  default void setSubstraitPlan(ByteBuffer plan) {
    throw new UnsupportedOperationException("Statement does not support Substrait plans");
  }

  /** Bind this statement to a VectorSchemaRoot to provide parameter values/bulk data ingestion. */
  default void bind(VectorSchemaRoot root) {
    throw new UnsupportedOperationException("Statement does not support bind");
  }

  /** Execute the query. */
  void execute() throws AdbcException;

  /**
   * Get the result of executing a query.
   *
   * <p>Must be called after {@link #execute()}.
   *
   * @throws IllegalStateException if the statement has not been executed.
   */
  ArrowReader getArrowReader() throws AdbcException;

  /**
   * Get a list of partitions of the result set.
   *
   * <p>These can be serialized and deserialized for parallel and/or distributed fetching.
   *
   * <p>Must be called after {@link #execute()}.
   *
   * @throws IllegalStateException if the statement has not been executed.
   * @return The list of descriptors, or an empty list if unsupported.
   */
  default List<PartitionDescriptor> getPartitionDescriptors() {
    // TODO: throw UnsupportedOperationException instead?
    return Collections.emptyList();
  }

  /**
   * Turn this statement into a prepared statement.
   *
   * <p>Call {@link #execute()} to execute the statement.
   */
  void prepare() throws AdbcException;
}
