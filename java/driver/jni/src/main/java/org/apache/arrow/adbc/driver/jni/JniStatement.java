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

package org.apache.arrow.adbc.driver.jni;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.jni.impl.ChildReferences;
import org.apache.arrow.adbc.driver.jni.impl.HasChildReferences;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativePartitionResult;
import org.apache.arrow.adbc.driver.jni.impl.NativeQueryResult;
import org.apache.arrow.adbc.driver.jni.impl.NativeStatementHandle;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

public class JniStatement implements AdbcStatement, HasChildReferences {
  private final BufferAllocator allocator;
  private final NativeStatementHandle handle;
  private final ChildReferences childReferences;
  // Hold the owning connection alive, and try to ensure this statement gets cleaned up before the
  // connection does
  private @Nullable HasChildReferences parent;
  private @Nullable VectorSchemaRoot bindRoot;
  private @Nullable ArrowReader bindStream;

  public JniStatement(
      BufferAllocator allocator, HasChildReferences parent, NativeStatementHandle handle) {
    this.allocator = allocator;
    this.handle = handle;
    this.childReferences = new ChildReferences();
    this.parent = parent;
    parent.getChildReferences().addReference(this);
  }

  @Override
  public ChildReferences getChildReferences() {
    return childReferences;
  }

  @Override
  public void cancel() throws AdbcException {
    JniLoader.INSTANCE.statementCancel(handle);
  }

  @Override
  public void setSqlQuery(String query) throws AdbcException {
    JniLoader.INSTANCE.statementSetSqlQuery(handle, query);
  }

  @Override
  public void setSubstraitPlan(ByteBuffer plan) throws AdbcException {
    JniLoader.INSTANCE.statementSetSubstraitPlan(handle, plan);
  }

  @Override
  public void bind(VectorSchemaRoot root) throws AdbcException {
    clearBind();
    this.bindRoot = root;
  }

  @Override
  public void bind(ArrowReader reader) throws AdbcException {
    clearBind();
    this.bindRoot = null;
    this.bindStream = reader;
  }

  private void clearBind() throws AdbcException {
    if (this.bindStream != null) {
      try {
        this.bindStream.close();
      } catch (IOException e) {
        throw AdbcException.internal("[jni] failed to close previous bind stream").withCause(e);
      } finally {
        this.bindStream = null;
      }
    }
    this.bindRoot = null;
  }

  // The C Data export takes ownership of the data at bind time and ignores subsequent
  // client changes to the bound root. Defer the export until execution so we capture
  // the final state of the VectorSchemaRoot.

  private void exportBind() throws AdbcException {
    if (bindRoot != null) {
      try (final ArrowArray batch = ArrowArray.allocateNew(allocator);
          final ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        // TODO(lidavidm): we may need a way to separately provide a dictionary provider
        Data.exportSchema(allocator, bindRoot.getSchema(), null, schema);
        Data.exportVectorSchemaRoot(allocator, bindRoot, null, batch);

        JniLoader.INSTANCE.statementBind(handle, batch, schema);
      }
    } else if (bindStream != null) {
      try (final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, bindStream, stream);
        JniLoader.INSTANCE.statementBindStream(handle, stream);
        // now owned by the native handle
        bindStream = null;
      }
    }
  }

  @Override
  public PartitionResult executePartitioned() throws AdbcException {
    exportBind();
    NativePartitionResult result = JniLoader.INSTANCE.statementExecutePartitions(handle);
    return result.importResult(allocator);
  }

  @Override
  public QueryResult executeQuery() throws AdbcException {
    exportBind();
    NativeQueryResult result = JniLoader.INSTANCE.statementExecuteQuery(handle);
    return new QueryResult(result.rowsAffected(), result.importStream(allocator, this));
  }

  @Override
  public UpdateResult executeUpdate() throws AdbcException {
    exportBind();
    long rowsAffected = JniLoader.INSTANCE.statementExecuteUpdate(handle);
    return new UpdateResult(rowsAffected);
  }

  @Override
  public Schema executeSchema() throws AdbcException {
    exportBind();
    return JniLoader.INSTANCE.statementExecuteSchema(handle).importSchema(allocator);
  }

  @Override
  public Schema getParameterSchema() throws AdbcException {
    return JniLoader.INSTANCE.statementGetParameterSchema(handle).importSchema(allocator);
  }

  @Override
  public void prepare() throws AdbcException {
    JniLoader.INSTANCE.statementPrepare(handle);
  }

  public double getProgress() throws AdbcException {
    return getOption(JniDriver.PROGRESS);
  }

  @Override
  public double getMaxProgress() throws AdbcException {
    return getOption(JniDriver.MAX_PROGRESS);
  }

  @Override
  public Iterator<PartitionResult> pollPartitioned() throws AdbcException {
    exportBind();
    setOption(JniDriver.INCREMENTAL, true);
    return new PartitionPollIterator();
  }

  @Override
  public void close() throws AdbcException {
    try {
      AutoCloseables.close(childReferences, handle, bindStream);
    } catch (Exception e) {
      throw AdbcException.internal("[jni] failed to close statement").withCause(e);
    } finally {
      final var parent = this.parent;
      if (parent != null) {
        parent.getChildReferences().releaseReference(this);
        this.parent = null;
      }
    }
  }

  @Override
  public <T> T getOption(TypedKey<T> key) throws AdbcException {
    if (key.getType() == String.class) {
      return key.cast(JniLoader.INSTANCE.statementGetOptionString(handle, key.getKey()));
    } else if (key.getType() == Integer.class) {
      return key.cast((int) JniLoader.INSTANCE.statementGetOptionLong(handle, key.getKey()));
    } else if (key.getType() == Long.class) {
      return key.cast(JniLoader.INSTANCE.statementGetOptionLong(handle, key.getKey()));
    } else if (key.getType() == Float.class) {
      return key.cast((float) JniLoader.INSTANCE.statementGetOptionDouble(handle, key.getKey()));
    } else if (key.getType() == Double.class) {
      return key.cast(JniLoader.INSTANCE.statementGetOptionDouble(handle, key.getKey()));
    } else if (key.getType() == Boolean.class) {
      String value = JniLoader.INSTANCE.statementGetOptionString(handle, key.getKey());
      if (value == null) {
        return null;
      } else if ("true".equalsIgnoreCase(value)) {
        return key.cast(Boolean.TRUE);
      } else if ("false".equalsIgnoreCase(value)) {
        return key.cast(Boolean.FALSE);
      } else {
        throw AdbcException.invalidArgument(
            "[jni] invalid boolean value for option " + key.getKey() + ": " + value);
      }
    } else if (key.getType() == byte[].class) {
      return key.cast(JniLoader.INSTANCE.statementGetOptionBytes(handle, key.getKey()));
    }
    return AdbcStatement.super.getOption(key);
  }

  @Override
  public <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
    if (value instanceof String) {
      JniLoader.INSTANCE.statementSetOptionString(handle, key.getKey(), (String) value);
    } else if (value == null) {
      JniLoader.INSTANCE.statementSetOptionString(handle, key.getKey(), null);
    } else if (value instanceof Integer) {
      JniLoader.INSTANCE.statementSetOptionLong(handle, key.getKey(), (Integer) value);
    } else if (value instanceof Long) {
      JniLoader.INSTANCE.statementSetOptionLong(handle, key.getKey(), (Long) value);
    } else if (value instanceof Float) {
      JniLoader.INSTANCE.statementSetOptionDouble(handle, key.getKey(), (Float) value);
    } else if (value instanceof Double) {
      JniLoader.INSTANCE.statementSetOptionDouble(handle, key.getKey(), (Double) value);
    } else if (value instanceof Boolean) {
      JniLoader.INSTANCE.statementSetOptionString(
          handle, key.getKey(), ((Boolean) value).toString());
    } else if (value instanceof byte[]) {
      JniLoader.INSTANCE.statementSetOptionBytes(handle, key.getKey(), (byte[]) value);
    } else {
      throw AdbcException.invalidArgument(
          "[jni] unsupported statement option type " + value.getClass());
    }
  }

  public final class PartitionPollIterator implements Iterator<PartitionResult> {
    @Nullable PartitionResult nextResult;
    boolean finished = false;

    @Override
    public boolean hasNext() {
      if (finished) return false;
      if (nextResult == null) {
        NativePartitionResult result;
        try {
          result = JniLoader.INSTANCE.statementExecutePartitions(handle);
        } catch (AdbcException e) {
          throw new RuntimeException(e);
        }
        nextResult = result.importResult(allocator);
      }
      if (nextResult.getPartitionDescriptors().isEmpty()) {
        finished = true;
        return false;
      }
      return true;
    }

    @Override
    public PartitionResult next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more partitions");
      }
      PartitionResult result = nextResult;
      nextResult = null;
      return result;
    }
  }
}
