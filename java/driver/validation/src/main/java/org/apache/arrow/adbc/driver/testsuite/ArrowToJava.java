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

package org.apache.arrow.adbc.driver.testsuite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class ArrowToJava {
  private ArrowToJava() {
    throw new AssertionError();
  }

  public static List<@Nullable Long> toLongs(List<@Nullable Long> result, ValueVector vector) {
    if (vector instanceof IntVector) {
      IntVector v = (IntVector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add((long) v.get(i));
        }
      }
      return result;
    } else if (vector instanceof BigIntVector) {
      BigIntVector v = (BigIntVector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add(v.get(i));
        }
      }
      return result;
    } else if (vector instanceof UInt4Vector) {
      UInt4Vector v = (UInt4Vector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add(Integer.toUnsignedLong(v.get(i)));
        }
      }
      return result;
    } else {
      throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
    }
  }

  public static List<@Nullable Long> toLongs(ValueVector vector) {
    return toLongs(new ArrayList<>(), vector);
  }

  public static List<@Nullable Long> toLongs(ArrowReader reader, String fieldName)
      throws IOException {
    return collect(reader, fieldName, ArrowToJava::toLongs);
  }

  public static List<@Nullable Integer> toIntegers(
      List<@Nullable Integer> result, ValueVector vector) {
    if (vector instanceof IntVector) {
      IntVector v = (IntVector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add(v.get(i));
        }
      }
      return result;
    } else if (vector instanceof UInt4Vector) {
      UInt4Vector v = (UInt4Vector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add(v.get(i));
        }
      }
      return result;
    } else {
      throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
    }
  }

  public static List<@Nullable Integer> toIntegers(ValueVector vector) {
    return toIntegers(new ArrayList<>(), vector);
  }

  public static List<@Nullable Integer> toIntegers(ArrowReader reader, String fieldName)
      throws IOException {
    return collect(reader, fieldName, ArrowToJava::toIntegers);
  }

  public static List<@Nullable String> toStrings(
      List<@Nullable String> result, ValueVector vector) {
    if (vector instanceof VarCharVector) {
      VarCharVector v = (VarCharVector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add(v.getObject(i).toString());
        }
      }
    } else if (vector instanceof LargeVarCharVector) {
      LargeVarCharVector v = (LargeVarCharVector) vector;
      for (int i = 0; i < v.getValueCount(); i++) {
        if (v.isNull(i)) {
          result.add(null);
        } else {
          result.add(v.getObject(i).toString());
        }
      }
    } else {
      throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
    }
    return result;
  }

  public static List<@Nullable String> toStrings(ValueVector vector) {
    return toStrings(new ArrayList<>(), vector);
  }

  public static List<@Nullable String> toStrings(ArrowReader reader, String fieldName)
      throws IOException {
    return collect(reader, fieldName, ArrowToJava::toStrings);
  }

  public static List<@Nullable Object> toObjects(
      List<@Nullable Object> result, ValueVector vector) {
    for (int i = 0; i < vector.getValueCount(); i++) {
      result.add(getObject(vector, i));
    }
    return result;
  }

  public static List<@Nullable Object> toObjects(ValueVector vector) {
    return toObjects(new ArrayList<>(), vector);
  }

  public static List<@Nullable Object> toObjects(ArrowReader reader, String fieldName)
      throws IOException {
    return collect(reader, fieldName, ArrowToJava::toObjects);
  }

  static <T> List<@Nullable T> collect(
      ArrowReader reader, String fieldName, ValueVectorCollector<T> collector) throws IOException {
    List<@Nullable T> result = new ArrayList<>();
    while (reader.loadNextBatch()) {
      collector.collect(result, reader.getVectorSchemaRoot().getVector(fieldName));
    }
    return result;
  }

  @FunctionalInterface
  interface ValueVectorCollector<T> {
    void collect(List<@Nullable T> result, ValueVector vector);
  }

  static @Nullable Object getObject(BigIntVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      return vector.getObject(index);
    }
  }

  static @Nullable Object getObject(BitVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      return vector.getObject(index);
    }
  }

  static @Nullable Object getObject(DenseUnionVector vector, int index) {
    byte typeId = vector.getTypeId(index);
    int offset = vector.getOffset(index);
    return getObject(vector.getVectorByType(typeId), offset);
  }

  static @Nullable Object getObject(IntVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      return vector.getObject(index);
    }
  }

  static @Nullable Object getObject(ListVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      List<@Nullable Object> vals = new ArrayList<>();
      ValueVector children = vector.getDataVector();
      int start = vector.getOffsetBuffer().getInt(index * 4L);
      int end = vector.getOffsetBuffer().getInt((index + 1) * 4L);
      for (int i = start; i < end; ++i) {
        vals.add(getObject(children, i));
      }
      return vals;
    }
  }

  static @Nullable Object getObject(SmallIntVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      return vector.getObject(index);
    }
  }

  static @Nullable Object getObject(StructVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      Map<String, @Nullable Object> m = new HashMap<>();
      for (String field : vector.getChildFieldNames()) {
        var child = vector.getChild(field);
        Object childValue = getObject(child, index);
        m.put(field, childValue);
      }
      return m;
    }
  }

  static @Nullable Object getObject(VarCharVector vector, int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      return vector.getObject(index).toString();
    }
  }

  static @Nullable Object getObject(ValueVector vector, int index) {
    if (vector instanceof BigIntVector) {
      return getObject((BigIntVector) vector, index);
    } else if (vector instanceof BitVector) {
      return getObject((BitVector) vector, index);
    } else if (vector instanceof DenseUnionVector) {
      return getObject((DenseUnionVector) vector, index);
    } else if (vector instanceof IntVector) {
      return getObject((IntVector) vector, index);
    } else if (vector instanceof ListVector) {
      return getObject((ListVector) vector, index);
    } else if (vector instanceof SmallIntVector) {
      return getObject((SmallIntVector) vector, index);
    } else if (vector instanceof StructVector) {
      return getObject((StructVector) vector, index);
    } else if (vector instanceof VarCharVector) {
      return getObject((VarCharVector) vector, index);
    } else {
      throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass());
    }
  }
}
