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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
public interface AdbcStatement extends AdbcCloseable, AdbcOptions {
  /**
   * Cancel execution of a query.
   *
   * <p>This can be used to interrupt execution of a method like {@link #executeQuery()}.
   *
   * <p>This method must be thread-safe (other method are not necessarily thread-safe).
   *
   * @since ADBC API revision 1.1.0
   */
  default void cancel() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support cancel");
  }

  /**
   * Set a generic query option.
   *
   * @deprecated Prefer {@link #setOption(TypedKey, Object)}.
   */
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
   * Execute a result set-generating query and get the result.
   *
   * <p>This may invalidate any prior result sets.
   */
  QueryResult executeQuery() throws AdbcException;

  /**
   * Execute a query.
   *
   * <p>This may invalidate any prior result sets.
   */
  UpdateResult executeUpdate() throws AdbcException;

  /**
   * Execute a result set-generating query and get a list of partitions of the result set.
   *
   * <p>These can be serialized and deserialized for parallel and/or distributed fetching.
   *
   * <p>This may invalidate any prior result sets.
   */
  default PartitionResult executePartitioned() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support executePartitioned");
  }

  /**
   * Get the schema of the result set without executing the query.
   *
   * @since ADBC API revision 1.1.0
   */
  default Schema executeSchema() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support executeSchema");
  }

  /**
   * Execute a result set-generating query and get a list of partitions of the result set.
   *
   * <p>These can be serialized and deserialized for parallel and/or distributed fetching.
   *
   * <p>This may invalidate any prior result sets.
   *
   * @since ADBC API revision 1.1.0
   */
  default Iterator<PartitionResult> pollPartitioned() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support pollPartitioned");
  }

  /**
   * Get the progress of executing a query.
   *
   * @since ADBC API revision 1.1.0
   */
  default double getProgress() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support getProgress");
  }

  /**
   * Get the upper bound of the progress.
   *
   * @since ADBC API revision 1.1.0
   */
  default double getMaxProgress() throws AdbcException {
    throw AdbcException.notImplemented("Statement does not support getMaxProgress");
  }

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
   * Turn this statement into a prepared statement.
   *
   * <p>Call {@link #executeQuery()}, {@link #executeUpdate()}, or {@link #executePartitioned()} to
   * execute the query.
   */
  void prepare() throws AdbcException;

  /** The result of executing a query with a result set. */
  class QueryResult implements AdbcCloseable {
    private final long affectedRows;

    private final ArrowReader reader;

    public QueryResult(long affectedRows, ArrowReader reader) {
      this.affectedRows = affectedRows;
      this.reader = reader;
    }

    public long getAffectedRows() {
      return affectedRows;
    }

    public ArrowReader getReader() {
      return reader;
    }

    @Override
    public String toString() {
      return "QueryResult{" + "affectedRows=" + affectedRows + ", reader=" + reader + '}';
    }

    /** Close the contained reader. */
    @Override
    public void close() throws AdbcException {
      try {
        reader.close();
      } catch (IOException e) {
        throw AdbcException.io(e);
      }
    }
  }

  /** The result of executing a query without a result set. */
  class UpdateResult {
    private final long affectedRows;

    public UpdateResult(long affectedRows) {
      this.affectedRows = affectedRows;
    }

    public long getAffectedRows() {
      return affectedRows;
    }

    @Override
    public String toString() {
      return "UpdateResult{" + "affectedRows=" + affectedRows + '}';
    }
  }

  /** The partitions of a result set. */
  class PartitionResult {
    private final Schema schema;
    private final long affectedRows;
    private final List<PartitionDescriptor> partitionDescriptors;

    public PartitionResult(
        Schema schema, long affectedRows, List<PartitionDescriptor> partitionDescriptors) {
      this.schema = schema;
      this.affectedRows = affectedRows;
      this.partitionDescriptors = partitionDescriptors;
    }

    /** Get the schema of the eventual result set. */
    public Schema getSchema() {
      return schema;
    }

    /** Get the number of affected rows, or -1 if not known. */
    public long getAffectedRows() {
      return affectedRows;
    }

    /** Get partitions. */
    public List<PartitionDescriptor> getPartitionDescriptors() {
      return partitionDescriptors;
    }

    @Override
    public String toString() {
      return "Partitions{"
          + "schema="
          + schema
          + ", affectedRows="
          + affectedRows
          + ", partitionDescriptors="
          + partitionDescriptors
          + '}';
    }
  }
}
