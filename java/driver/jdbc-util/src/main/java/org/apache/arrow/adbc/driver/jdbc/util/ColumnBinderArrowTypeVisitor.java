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

import org.apache.arrow.vector.types.pojo.ArrowType;

final class ColumnBinderArrowTypeVisitor implements ArrowType.ArrowTypeVisitor<ColumnBinder> {
  private final boolean nullable;

  public ColumnBinderArrowTypeVisitor(boolean nullable) {
    this.nullable = nullable;
  }

  @Override
  public ColumnBinder visit(ArrowType.Null type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Struct type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.List type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.LargeList type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.FixedSizeList type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Union type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Map type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Int type) {
    switch (type.getBitWidth()) {
      case 8:
        if (type.getIsSigned()) {
          return nullable
              ? ColumnBinders.NullableTinyIntBinder.INSTANCE
              : ColumnBinders.TinyIntBinder.INSTANCE;
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      case 16:
        if (type.getIsSigned()) {
          return nullable
              ? ColumnBinders.NullableSmallIntBinder.INSTANCE
              : ColumnBinders.SmallIntBinder.INSTANCE;
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      case 32:
        if (type.getIsSigned()) {
          return nullable
              ? ColumnBinders.NullableIntBinder.INSTANCE
              : ColumnBinders.IntBinder.INSTANCE;
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      case 64:
        if (type.getIsSigned()) {
          return nullable
              ? ColumnBinders.NullableBigIntBinder.INSTANCE
              : ColumnBinders.BigIntBinder.INSTANCE;
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
    }
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.FloatingPoint type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Utf8 type) {
    return ColumnBinders.NullableVarCharBinder.INSTANCE;
  }

  @Override
  public ColumnBinder visit(ArrowType.LargeUtf8 type) {
    return ColumnBinders.NullableLargeVarCharBinder.INSTANCE;
  }

  @Override
  public ColumnBinder visit(ArrowType.Binary type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.LargeBinary type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.FixedSizeBinary type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Bool type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Decimal type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Date type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Time type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Timestamp type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Interval type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Duration type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }
}
