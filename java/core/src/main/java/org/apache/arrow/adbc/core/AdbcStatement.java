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
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A container for all state needed to execute a database query, such as the query itself,
 * parameters for prepared statements, driver parameters, etc.
 *
 * <p>Statements may represent queries or prepared statements.
 *
 * <p>Statements may be used multiple times and can be reconfigured (e.g. they can be reused to
 * execute multiple different queries). However, executing a statement (and changing certain other
 * state) will invalidate result sets obtained prior to that execution.
 *
 * <p>Multiple statements may be created from a single connection. However, the driver may block or
 * error if they are used concurrently (whether from a single thread or multiple threads).
 *
 * <p>Statements are not required to be thread-safe, but they can be used from multiple threads so
 * long as clients take care to serialize accesses to a statement.
 */
public interface AdbcStatement extends AutoCloseable {
  /** Set a generic query option. */
  default void setOption(String key, Object value) throws AdbcException {
    throw AdbcException.notImplemented("Unsupported option " + key);
  }

  /**
   * Set a SQL query to be executed on this statement.
   *
   * @param query The SQL query.
   */
  default void setSqlQuery(String query) throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support SQL queries");
  }

  /**
   * Set a Substrait plan to be executed on this statement.
   *
   * @param plan The serialized Substrait plan.
   */
  default void setSubstraitPlan(ByteBuffer plan) throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support Substrait plans");
  }

  /** Bind this statement to a VectorSchemaRoot to provide parameter values/bulk data ingestion. */
  default void bind(VectorSchemaRoot root) throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support bind");
  }

  /**
   * Execute the query.
   *
   * <p>Usually you will want to use {@link #executeQuery()}.
   *
   * <p>This invalidates any prior result sets.
   */
  void execute() throws AdbcException;

  /**
   * Execute a result set-generating query and get the result.
   *
   * <p>This invalidates any prior result sets.
   */
  default ArrowReader executeQuery() throws AdbcException {
    execute();
    return getArrowReader();
  }

  /**
   * Get the result of executing a query.
   *
   * <p>Must be called after {@link #execute()}.
   *
   * @throws AdbcException with {@link AdbcStatusCode#INVALID_STATE} if the statement has not been
   *     executed.
   */
  ArrowReader getArrowReader() throws AdbcException;

  /**
   * Get the schema for bound parameters.
   *
   * <p>This retrieves an Arrow schema describing the number, names, and types of the parameters in
   * a parameterized statement. The fields of the schema should be in order of the ordinal position
   * of the parameters; named parameters should appear only once.
   *
   * <p>If the parameter does not have a name, or the name cannot be determined, the name of the
   * corresponding field in the schema will be an empty string. If the type cannot be determined,
   * the type of the corresponding field will be NA (NullType).
   *
   * <p>This should be called after AdbcStatementPrepare.
   *
   * @throws AdbcException with {@link AdbcStatusCode#NOT_IMPLEMENTED} if the parameters cannot be
   *     determined at all.
   */
  default Schema getParameterSchema() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support getParameterSchema");
  }

  /**
   * Get a list of partitions of the result set.
   *
   * <p>These can be serialized and deserialized for parallel and/or distributed fetching.
   *
   * <p>Must be called after {@link #execute()}.
   *
   * @throws AdbcException with {@link AdbcStatusCode#INVALID_STATE} if the statement has not been
   *     executed.
   * @return The list of descriptors, or an empty list if unsupported.
   */
  default List<PartitionDescriptor> getPartitionDescriptors() throws AdbcException {
    return Collections.emptyList();
  }

  /**
   * Turn this statement into a prepared statement.
   *
   * <p>Call {@link #execute()} to execute the statement.
   */
  void prepare() throws AdbcException;
}
