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
import java.sql.Types;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;

/** Column binder implementations. */
final class ColumnBinders {
  // While tedious to define all of these, this avoids unnecessary boxing/unboxing and limits the
  // call depth compared to trying to be generic with lambdas (hopefully makes inlining easier).
  // We can consider code templating like arrow-vector for maintenance.

  // ------------------------------------------------------------
  // Int8

  enum NullableTinyIntBinder implements ColumnBinder {
    INSTANCE;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      if (vector.isNull(rowIndex)) {
        statement.setNull(parameterIndex, Types.TINYINT);
      } else {
        statement.setByte(parameterIndex, ((TinyIntVector) vector).get(rowIndex));
      }
    }
  }

  enum TinyIntBinder implements ColumnBinder {
    INSTANCE;

    private static final long BYTE_WIDTH = 1;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      final byte value = vector.getDataBuffer().getByte(rowIndex * BYTE_WIDTH);
      statement.setByte(parameterIndex, value);
    }
  }

  // ------------------------------------------------------------
  // Int16

  enum NullableSmallIntBinder implements ColumnBinder {
    INSTANCE;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      if (vector.isNull(rowIndex)) {
        statement.setNull(parameterIndex, Types.SMALLINT);
      } else {
        statement.setShort(parameterIndex, ((SmallIntVector) vector).get(rowIndex));
      }
    }
  }

  enum SmallIntBinder implements ColumnBinder {
    INSTANCE;

    private static final long BYTE_WIDTH = 2;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      final byte value = vector.getDataBuffer().getByte(rowIndex * BYTE_WIDTH);
      statement.setByte(parameterIndex, value);
    }
  }

  // ------------------------------------------------------------
  // Int32

  enum NullableIntBinder implements ColumnBinder {
    INSTANCE;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      if (vector.isNull(rowIndex)) {
        statement.setNull(parameterIndex, Types.INTEGER);
      } else {
        statement.setInt(parameterIndex, ((IntVector) vector).get(rowIndex));
      }
    }
  }

  enum IntBinder implements ColumnBinder {
    INSTANCE;

    private static final long BYTE_WIDTH = 4;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      final int value = vector.getDataBuffer().getInt(rowIndex * BYTE_WIDTH);
      statement.setInt(parameterIndex, value);
    }
  }

  // ------------------------------------------------------------
  // Int64

  enum NullableBigIntBinder implements ColumnBinder {
    INSTANCE;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      if (vector.isNull(rowIndex)) {
        statement.setNull(parameterIndex, Types.BIGINT);
      } else {
        statement.setLong(parameterIndex, ((BigIntVector) vector).get(rowIndex));
      }
    }
  }

  enum BigIntBinder implements ColumnBinder {
    INSTANCE;

    private static final long BYTE_WIDTH = 4;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      final long value = vector.getDataBuffer().getLong(rowIndex * BYTE_WIDTH);
      statement.setLong(parameterIndex, value);
    }
  }

  // ------------------------------------------------------------
  // String, LargeString

  // TODO: we may be able to do this generically via ElementAddressableVector
  // TODO: make this non-singleton so we don't have to allocate when using getDataPointer?
  // TODO: avoid getObject and just use the byte[] directly (or, can we directly get a String from
  // an ArrowBuf?)
  enum NullableVarCharBinder implements ColumnBinder {
    INSTANCE;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      if (vector.isNull(rowIndex)) {
        statement.setNull(parameterIndex, Types.VARCHAR);
      } else {
        statement.setString(
            parameterIndex, ((VarCharVector) vector).getObject(rowIndex).toString());
      }
    }
  }

  enum NullableLargeVarCharBinder implements ColumnBinder {
    INSTANCE;

    @Override
    public void bind(
        PreparedStatement statement, FieldVector vector, int parameterIndex, int rowIndex)
        throws SQLException {
      if (vector.isNull(rowIndex)) {
        statement.setNull(parameterIndex, Types.VARCHAR);
      } else {
        statement.setString(
            parameterIndex, ((LargeVarCharVector) vector).getObject(rowIndex).toString());
      }
    }
  }
}
