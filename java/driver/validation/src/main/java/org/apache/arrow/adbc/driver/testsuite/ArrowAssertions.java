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

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
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
      if (!actual.getSchema().equals(expectedRoot.getSchema())) {
        throw failureWithActualExpected(
            actual,
            expected,
            "Expected Schema:\n%sActual Schema:\n%s",
            expectedRoot.getSchema(),
            actual.getSchema());
      }

      for (int i = 0; i < expectedRoot.getSchema().getFields().size(); i++) {
        final FieldVector expectedVector = expectedRoot.getVector(i);
        final FieldVector actualVector = actual.getVector(i);
        if (!VectorEqualsVisitor.vectorEquals(expectedVector, actualVector)) {
          throw failureWithActualExpected(
              actual,
              expected,
              "Vector %s does not match.\nExpected vector: %s\nActual vector  : %s",
              expectedVector.getField(),
              expectedVector,
              actualVector);
        }
      }
      return this;
    }
  }
}
