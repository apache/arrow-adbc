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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcParameterBinderTest {
  BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  @Test
  void bindOrder() throws SQLException {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("ints0", new ArrowType.Int(32, true)),
                Field.nullable("ints1", new ArrowType.Int(32, true)),
                Field.nullable("ints2", new ArrowType.Int(32, true))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root)
              .bind(/*paramIndex=*/ 1, /*colIndex=*/ 2)
              .bind(/*paramIndex=*/ 2, /*colIndex=*/ 0)
              .build();
      assertThat(binder.next()).isFalse();

      final IntVector ints0 = (IntVector) root.getVector(0);
      final IntVector ints1 = (IntVector) root.getVector(1);
      final IntVector ints2 = (IntVector) root.getVector(2);
      ints0.setSafe(0, 4);
      ints0.setNull(1);
      ints1.setNull(0);
      ints1.setSafe(1, -8);
      ints2.setNull(0);
      ints2.setSafe(1, 12);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.INTEGER);
      assertThat(statement.getParamValue(2)).isInstanceOf(Integer.class).isEqualTo(4);
      assertThat(statement.getParam(3)).isNull();
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isInstanceOf(Integer.class).isEqualTo(12);
      assertThat(statement.getParamValue(2)).isNull();
      assertThat(statement.getParamType(2)).isEqualTo(Types.INTEGER);
      assertThat(statement.getParam(3)).isNull();
      assertThat(binder.next()).isFalse();

      binder.reset();

      ints0.setNull(0);
      ints0.setSafe(1, -2);
      ints2.setNull(0);
      ints2.setSafe(1, 6);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.INTEGER);
      assertThat(statement.getParamValue(2)).isNull();
      assertThat(statement.getParamType(2)).isEqualTo(Types.INTEGER);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isInstanceOf(Integer.class).isEqualTo(6);
      assertThat(statement.getParamValue(2)).isInstanceOf(Integer.class).isEqualTo(-2);
      assertThat(statement.getParam(3)).isNull();
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void customBinder() throws SQLException {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints0", new ArrowType.Int(32, true))));

    try (final MockPreparedStatement statement = new MockPreparedStatement();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root)
              .bind(
                  /*paramIndex=*/ 1,
                  /*colIndex=*/ 0,
                  (statement1, vector, parameterIndex, rowIndex) -> {
                    Integer value = ((IntVector) vector).getObject(rowIndex);
                    if (value == null) {
                      statement1.setString(parameterIndex, "null");
                    } else {
                      statement1.setString(parameterIndex, Integer.toString(value));
                    }
                  })
              .build();
      assertThat(binder.next()).isFalse();

      final IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, 4);
      ints.setNull(1);

      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo("4");
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo("null");
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void int8() throws SQLException {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(8, true))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      final TinyIntVector ints = (TinyIntVector) root.getVector(0);
      ints.setSafe(0, Byte.MIN_VALUE);
      ints.setSafe(1, Byte.MAX_VALUE);
      ints.setNull(2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Byte.MIN_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Byte.MAX_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.TINYINT);
      assertThat(binder.next()).isFalse();

      binder.reset();

      ints.setNull(0);
      ints.setSafe(1, 2);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.TINYINT);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo((byte) 2);
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void int16() throws SQLException {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(16, true))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      final SmallIntVector ints = (SmallIntVector) root.getVector(0);
      ints.setSafe(0, Short.MIN_VALUE);
      ints.setSafe(1, Short.MAX_VALUE);
      ints.setNull(2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Short.MIN_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Short.MAX_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.SMALLINT);
      assertThat(binder.next()).isFalse();

      binder.reset();

      ints.setNull(0);
      ints.setSafe(1, 2);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.SMALLINT);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo((short) 2);
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void int32() throws SQLException {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      final IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, Integer.MIN_VALUE);
      ints.setSafe(1, Integer.MAX_VALUE);
      ints.setNull(2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Integer.MIN_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Integer.MAX_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.INTEGER);
      assertThat(binder.next()).isFalse();

      binder.reset();

      ints.setNull(0);
      ints.setSafe(1, 2);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.INTEGER);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(2);
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void int64() throws SQLException {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(64, true))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      final BigIntVector ints = (BigIntVector) root.getVector(0);
      ints.setSafe(0, Long.MIN_VALUE);
      ints.setSafe(1, Long.MAX_VALUE);
      ints.setNull(2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Long.MIN_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(Long.MAX_VALUE);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.BIGINT);
      assertThat(binder.next()).isFalse();

      binder.reset();

      ints.setNull(0);
      ints.setSafe(1, 2);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.BIGINT);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(2L);
      assertThat(binder.next()).isFalse();
    }
  }
}
