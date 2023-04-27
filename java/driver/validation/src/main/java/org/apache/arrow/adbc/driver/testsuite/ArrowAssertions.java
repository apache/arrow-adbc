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

import java.util.List;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.AbstractAssert;

/** AssertJ assertions for Arrow. */
public final class ArrowAssertions {
  /** Assert on a {@link AdbcException}. */
  public static AdbcExceptionAssert assertAdbcException(AdbcException actual) {
    return new AdbcExceptionAssert(actual);
  }

  /** Assert on a {@link VectorSchemaRoot}. */
  public static VectorSchemaRootAssert assertRoot(VectorSchemaRoot actual) {
    return new VectorSchemaRootAssert(actual);
  }

  public static SchemaAssert assertSchema(Schema actual) {
    return new SchemaAssert(actual);
  }

  public static FieldAssert assertField(Field actual) {
    return new FieldAssert(actual);
  }

  public static final class AdbcExceptionAssert
      extends AbstractAssert<AdbcExceptionAssert, AdbcException> {
    AdbcExceptionAssert(AdbcException e) {
      super(e, AdbcExceptionAssert.class);
    }

    public AdbcExceptionAssert isStatus(AdbcStatusCode status) {
      if (actual.getStatus() != status) {
        throw failureWithActualExpected(
            actual.getStatus(),
            status,
            "Expected status %s but got %s:\n%s",
            status,
            actual.getStatus(),
            actual);
      }
      return this;
    }
  }

  public static final class VectorSchemaRootAssert
      extends AbstractAssert<VectorSchemaRootAssert, VectorSchemaRoot> {
    VectorSchemaRootAssert(VectorSchemaRoot vectorSchemaRoot) {
      super(vectorSchemaRoot, VectorSchemaRootAssert.class);
    }

    @Override
    public VectorSchemaRootAssert isEqualTo(Object expected) {
      if (!(expected instanceof VectorSchemaRoot)) {
        throw failure(
            "Expected object is not a VectorSchemaRoot, but rather a %s",
            expected.getClass().getName());
      }
      final VectorSchemaRoot expectedRoot = (VectorSchemaRoot) expected;
      assertSchema(actual.getSchema()).isEqualTo(expectedRoot.getSchema());

      for (int i = 0; i < expectedRoot.getSchema().getFields().size(); i++) {
        final FieldVector expectedVector = expectedRoot.getVector(i);
        final FieldVector actualVector = actual.getVector(i);
        if (!VectorEqualsVisitor.vectorEquals(
            expectedVector,
            actualVector,
            (v1, v2) ->
                new TypeEqualsVisitor(v2, /*checkName*/ false, /*checkMetadata*/ false)
                    .equals(v1))) {
          throw failureWithActualExpected(
              actual,
              expected,
              "Vector %s does not match %s.\nExpected vector: %s\nActual vector  : %s",
              expectedVector.getField(),
              actualVector.getField(),
              expectedVector,
              actualVector);
        }
      }
      return this;
    }
  }

  public static class SchemaAssert extends AbstractAssert<SchemaAssert, Schema> {
    SchemaAssert(Schema schema) {
      super(schema, SchemaAssert.class);
    }

    @Override
    public SchemaAssert isEqualTo(Object expected) {
      if (!(expected instanceof Schema)) {
        throw failure(
            "Expected object is not a Schema, but rather a %s", expected.getClass().getName());
      }
      final Schema expectedSchema = (Schema) expected;
      if (!schemasEqualIgnoringMetadata(expectedSchema, actual)) {
        throw failureWithActualExpected(
            actual, expected, "Expected Schema:\n%s\nActual Schema:\n%s", expectedSchema, actual);
      }
      return this;
    }

    private boolean schemasEqualIgnoringMetadata(Schema expected, Schema actual) {
      if (expected.getFields().size() != actual.getFields().size()) {
        return false;
      }
      for (int i = 0; i < expected.getFields().size(); i++) {
        assertField(actual.getFields().get(i)).isEqualTo(expected.getFields().get(i));
      }
      return true;
    }
  }

  public static class FieldAssert extends AbstractAssert<FieldAssert, Field> {
    FieldAssert(Field field) {
      super(field, FieldAssert.class);
    }

    @Override
    public FieldAssert isEqualTo(Object expected) {
      if (!(expected instanceof Field)) {
        throw failure(
            "Expected object is not a Field, but rather a %s", expected.getClass().getName());
      }
      final Field expectedField = (Field) expected;
      if (!fieldsEqualIgnoringMetadata(expectedField, actual)) {
        throw failureWithActualExpected(
            actual, expected, "Expected Field:\n%s\nActual Field:\n%s", expectedField, actual);
      }
      return this;
    }

    private boolean fieldsEqualIgnoringMetadata(Field expectedField, Field actualField) {
      if (!expectedField.getName().equals(actualField.getName())) {
        return false;
      }

      if (!expectedField.getType().equals(actualField.getType())) {
        return false;
      }

      if (expectedField.getFieldType().isNullable() != actualField.getFieldType().isNullable()) {
        return false;
      }

      if (!Objects.equals(
          expectedField.getFieldType().getDictionary(),
          actualField.getFieldType().getDictionary())) {
        return false;
      }

      return fieldsEqualIgnoringMetadata(expectedField.getChildren(), actualField.getChildren());
    }

    private boolean fieldsEqualIgnoringMetadata(List<Field> expected, List<Field> actual) {
      if (expected.size() != actual.size()) {
        return false;
      }

      for (int i = 0; i < expected.size(); i++) {
        if (!fieldsEqualIgnoringMetadata(expected.get(i), actual.get(i))) {
          return false;
        }
      }

      return true;
    }
  }
}
