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

package org.apache.arrow.adbc.driver.jdbc.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Helper to bind values from an Arrow {@link VectorSchemaRoot} to a JDBC {@link PreparedStatement}.
 */
public class JdbcParameterBinder {
  private final PreparedStatement statement;
  private final VectorSchemaRoot root;
  private final ColumnBinder[] binders;
  private final int[] parameterIndices;
  private final int[] columnIndices;
  private int nextRowIndex;

  JdbcParameterBinder(
      final PreparedStatement statement,
      final VectorSchemaRoot root,
      final ColumnBinder[] binders,
      int[] parameterIndices,
      int[] columnIndices) {
    Preconditions.checkArgument(
        parameterIndices.length == columnIndices.length,
        "Length of parameter indices and column indices must match");
    this.statement = statement;
    this.root = root;
    this.binders = binders;
    this.parameterIndices = parameterIndices;
    this.columnIndices = columnIndices;
    this.nextRowIndex = 0;
  }

  /**
   * Create a builder.
   *
   * @param statement The statement to bind to.
   * @param root The root to pull data from.
   */
  public static Builder builder(final PreparedStatement statement, final VectorSchemaRoot root) {
    return new Builder(statement, root);
  }

  /** Reset the binder (so the root can be updated with new data). */
  public void reset() {
    nextRowIndex = 0;
  }

  /**
   * Bind the next row to the statement.
   *
   * @return true if a row was bound, false if rows were exhausted
   */
  public boolean next() throws SQLException {
    if (nextRowIndex >= root.getRowCount()) {
      return false;
    }
    for (int i = 0; i < parameterIndices.length; i++) {
      final int parameterIndex = parameterIndices[i];
      final int columnIndex = columnIndices[i];
      final FieldVector vector = root.getVector(columnIndex);
      binders[i].bind(statement, vector, parameterIndex, nextRowIndex);
    }
    nextRowIndex++;
    return true;
  }

  private static class Binding {
    int columnIndex;
    ColumnBinder binder;

    Binding(int columnIndex, ColumnBinder binder) {
      this.columnIndex = columnIndex;
      this.binder = binder;
    }
  }

  /** A builder for a {@link JdbcParameterBinder}. */
  public static class Builder {
    private final PreparedStatement statement;
    private final VectorSchemaRoot root;
    // Parameter index -> (Column Index, Binder)
    private final Map<Integer, Binding> bindings;

    Builder(PreparedStatement statement, VectorSchemaRoot root) {
      this.statement = statement;
      this.root = root;
      this.bindings = new HashMap<>();
    }

    /** Bind each column to the corresponding parameter in order. */
    public Builder bindAll() {
      for (int i = 0; i < root.getFieldVectors().size(); i++) {
        bind(/*parameterIndex=*/ i + 1, /*columnIndex=*/ i);
      }
      return this;
    }

    /** Bind the given parameter to the given column using the default binder. */
    public Builder bind(int parameterIndex, int columnIndex) {
      return bind(
          parameterIndex,
          columnIndex,
          ColumnBinder.forField(root.getVector(columnIndex).getField()));
    }

    /** Bind the given parameter to the given column using the given binder. */
    public Builder bind(int parameterIndex, int columnIndex, ColumnBinder binder) {
      Preconditions.checkArgument(
          parameterIndex > 0, "parameterIndex %d must be positive", parameterIndex);
      bindings.put(parameterIndex, new Binding(columnIndex, binder));
      return this;
    }

    /** Build the binder. */
    public JdbcParameterBinder build() {
      ColumnBinder[] binders = new ColumnBinder[bindings.size()];
      int[] parameterIndices = new int[bindings.size()];
      int[] columnIndices = new int[bindings.size()];
      final Stream<Map.Entry<Integer, Binding>> sortedBindings =
          bindings.entrySet().stream()
              .sorted(Comparator.comparingInt(entry -> entry.getValue().columnIndex));
      int index = 0;
      for (Map.Entry<Integer, Binding> entry :
          (Iterable<Map.Entry<Integer, Binding>>) sortedBindings::iterator) {
        binders[index] = entry.getValue().binder;
        parameterIndices[index] = entry.getKey();
        columnIndices[index] = entry.getValue().columnIndex;
        index++;
      }
      return new JdbcParameterBinder(statement, root, binders, parameterIndices, columnIndices);
    }
  }
}
