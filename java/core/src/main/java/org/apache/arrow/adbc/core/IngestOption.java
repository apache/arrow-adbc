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
package org.apache.arrow.adbc.core;

import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface IngestOption {
  TemporaryIngestOption TEMPORARY = new TemporaryIngestOption(true);
  TemporaryIngestOption NOT_TEMPORARY = new TemporaryIngestOption(false);

  static IngestOption targetCatalog(String catalog) {
    return new TargetNamespaceIngestOption(catalog, null);
  }

  static IngestOption targetDbSchema(String dbSchema) {
    return new TargetNamespaceIngestOption(null, dbSchema);
  }

  static IngestOption targetNamespace(@Nullable String catalog, @Nullable String dbSchema) {
    return new TargetNamespaceIngestOption(catalog, dbSchema);
  }

  class TemporaryIngestOption implements IngestOption {
    boolean temporary;

    TemporaryIngestOption(boolean temporary) {
      this.temporary = temporary;
    }

    public boolean isTemporary() {
      return temporary;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      TemporaryIngestOption that = (TemporaryIngestOption) o;
      return temporary == that.temporary;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(temporary);
    }
  }

  class TargetNamespaceIngestOption implements IngestOption {
    private final @Nullable String targetCatalog;
    private final @Nullable String targetDbSchema;

    public TargetNamespaceIngestOption(
        @Nullable String targetCatalog, @Nullable String targetDbSchema) {
      this.targetCatalog = targetCatalog;
      this.targetDbSchema = targetDbSchema;
    }

    public @Nullable String getTargetCatalog() {
      return targetCatalog;
    }

    public @Nullable String getTargetDbSchema() {
      return targetDbSchema;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      TargetNamespaceIngestOption that = (TargetNamespaceIngestOption) o;
      return Objects.equals(targetCatalog, that.targetCatalog)
          && Objects.equals(targetDbSchema, that.targetDbSchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(targetCatalog, targetDbSchema);
    }
  }
}
