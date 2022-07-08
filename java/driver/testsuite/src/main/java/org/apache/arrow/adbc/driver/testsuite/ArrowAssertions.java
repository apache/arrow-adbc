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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.AbstractAssert;

/** AssertJ assertions for Arrow. */
public final class ArrowAssertions {
  /** Assert on a {@link VectorSchemaRoot}. */
  public static VectorSchemaRootAssert assertRoot(VectorSchemaRoot actual) {
    return new VectorSchemaRootAssert(actual);
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
      if (!actual.equals(expectedRoot)) {
        throw failureWithActualExpected(
            actual,
            expected,
            "Expected Root:\n%sActual Root:\n%s",
            expectedRoot.contentToTSVString(),
            actual.contentToTSVString());
      }
      return this;
    }
  }
}
