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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/** A helper to bind values from an Arrow vector to a JDBC PreparedStatement. */
@FunctionalInterface
public interface ColumnBinder {
  /**
   * Bind the given row to the given parameter.
   *
   * @param statement The staement to bind to
   * @param vector The vector to pull data from
   * @param parameterIndex The parameter to bind to
   * @param rowIndex The row to bind
   * @throws SQLException if an error occurs
   */
  void bind(PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
      throws SQLException;

  /**
   * Get an instance of the default binder for a type.
   *
   * @throws UnsupportedOperationException if the type is not supported.
   */
  static ColumnBinder forType(ArrowType type) {
    return type.accept(new ColumnBinderArrowTypeVisitor(/*nullable=*/ true));
  }

  /**
   * Get an instance of the default binder for a type, accounting for nullability.
   *
   * @throws UnsupportedOperationException if the type is not supported.
   */
  static ColumnBinder forField(Field field) {
    return field.getType().accept(new ColumnBinderArrowTypeVisitor(field.isNullable()));
  }
}
