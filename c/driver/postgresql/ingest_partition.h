// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include <arrow-adbc/adbc.h>

namespace adbcpq {

// Wire format for the partitioned-ingest handle. Opaque to callers; symmetric
// across coordinator and workers.
struct IngestHandle {
  static constexpr std::array<uint8_t, 4> kMagic = {'P', 'I', 'H', '1'};

  std::array<uint8_t, 16> ingest_id;
  std::string catalog;
  std::string db_schema;
  std::string table;

  // staging table prefix shared by all writes scoped to this handle.
  // Driver-internal — used by Abort to enumerate orphans.
  std::string StagingPrefix() const;

  size_t SerializedSize() const;
  void Serialize(uint8_t* out) const;
  static AdbcStatusCode Parse(const uint8_t* bytes, size_t len, IngestHandle* out,
                              struct AdbcError* error);

  static void GenerateId(std::array<uint8_t, 16>* out);
};

// Wire format for a per-partition receipt.
struct IngestReceipt {
  static constexpr std::array<uint8_t, 4> kMagic = {'P', 'I', 'R', '1'};

  std::string staging_schema;  // empty -> default schema
  std::string staging_table;
  // Already-escaped, comma-separated column list (e.g. `"a", "b"`). Used by
  // Commit to construct INSERT INTO target (cols) SELECT cols FROM staging.
  std::string escaped_columns;
  int64_t row_count = 0;

  size_t SerializedSize() const;
  void Serialize(uint8_t* out) const;
  static AdbcStatusCode Parse(const uint8_t* bytes, size_t len, IngestReceipt* out,
                              struct AdbcError* error);
};

}  // namespace adbcpq
