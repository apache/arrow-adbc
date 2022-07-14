package org.apache.arrow.adbc.driver.testsuite;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public final class TestUtil {
  public static final Schema INTS_STRS_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("INTS", new ArrowType.Int(32, /*signed=*/ true)),
              Field.nullable("STRS", new ArrowType.Utf8())));

  /** Load a simple table with two columns. */
  public static void ingestTableIntsStrs(
      BufferAllocator allocator, AdbcConnection connection, String tableName) throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(INTS_STRS_SCHEMA, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      final VarCharVector strs = (VarCharVector) root.getVector(1);

      ints.allocateNew(4);
      ints.setSafe(0, 0);
      ints.setSafe(1, 1);
      ints.setSafe(2, 2);
      ints.setNull(3);
      strs.allocateNew(4);
      strs.setNull(0);
      strs.setSafe(1, "foo".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(2, "".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(3, "asdf".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(4);

      // TODO: XXX: need a "quirks" system to handle idiosyncracies. For example: Derby forces table
      // names to uppercase, but does not do case folding in all places.
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.execute();
      }
    }
  }
}
