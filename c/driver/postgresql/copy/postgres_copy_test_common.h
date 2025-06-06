// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>

namespace adbcpq {

// New cases can be generated using:
// psql --host 127.0.0.1 --port 5432 --username postgres -c "COPY (SELECT ...) TO STDOUT
// WITH (FORMAT binary);" > test.copy Rscript -e "dput(brio::read_file_raw('test.copy'))"

// COPY (SELECT CAST("col" AS BOOLEAN) AS "col" FROM (  VALUES (TRUE), (FALSE), (NULL)) AS
// drvd("col")) TO STDOUT;
static const uint8_t kTestPgCopyBoolean[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS SMALLINT) AS "col" FROM (  VALUES (-123), (-1), (1), (123),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopySmallInt[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0xff, 0x85, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0xff, 0xff, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS INTEGER) AS "col" FROM (  VALUES (-123), (-1), (1), (123),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyInteger[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
    0x85, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS BIGINT) AS "col" FROM (  VALUES (-123), (-1), (1), (123),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyBigInt[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0x85, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS REAL) AS "col" FROM (  VALUES (-123.456), (-1), (1),
// (123.456), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyReal[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xc2, 0xf6, 0xe9,
    0x79, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xbf, 0x80, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x04, 0x3f, 0x80, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x42,
    0xf6, 0xe9, 0x79, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS DOUBLE PRECISION) AS "col" FROM (  VALUES (-123.456), (-1),
// (1), (123.456), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyDoublePrecision[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xc0, 0x5e, 0xdd,
    0x2f, 0x1a, 0x9f, 0xbe, 0x77, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xbf, 0xf0, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x3f, 0xf0, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x40, 0x5e, 0xdd,
    0x2f, 0x1a, 0x9f, 0xbe, 0x77, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

static const uint8_t kTestPgCopyDate[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x04, 0xff, 0xff, 0x71, 0x54, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0x8e, 0xad, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS TIME) AS "col" FROM (  VALUES ('00:00:00'), ('23:59:59'),
// ('13:42:57.123456'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyTime[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x14, 0x1d, 0xc8, 0x1d, 0xc0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x0b, 0x7f, 0x0b, 0xda, 0x40, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST(col AS TIMESTAMP) FROM (  VALUES ('1900-01-01 12:34:56'),
// ('2100-01-01 12:34:56'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT BINARY);
static const uint8_t kTestPgCopyTimestamp[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff, 0xf4, 0xc9,
    0xf9, 0x07, 0xe5, 0x9c, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0b, 0x36,
    0x30, 0x2d, 0xa5, 0xfc, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST(col AS INTERVAL) FROM (  VALUES ('-1 months -2 days -4 seconds'),
// ('1 months 2 days 4 seconds'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT BINARY);
static const uint8_t kTestPgCopyInterval[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xc2, 0xf7, 0x00, 0xff, 0xff, 0xff,
    0xfe, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x3d, 0x09, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS TEXT) AS "col" FROM (  VALUES ('abc'), ('1234'),
// (NULL::text)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyText[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x03, 0x61, 0x62, 0x63, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x31, 0x32,
    0x33, 0x34, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST(col AS json) AS col FROM (VALUES ('[1, 2, 3]'), ('[4, 5, 6]'),
// (NULL::json)) AS drvd(col)) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyJson[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x09, 0x5b, 0x31, 0x2c, 0x20, 0x32, 0x2c, 0x20, 0x33, 0x5d, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x09, 0x5b, 0x34, 0x2c, 0x20, 0x35, 0x2c, 0x20, 0x36,
    0x5d, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST(col AS jsonb) AS col FROM (VALUES ('[1, 2, 3]'), ('[4, 5, 6]'),
// (NULL::jsonb)) AS drvd(col)) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyJsonb[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0a, 0x01, 0x5b, 0x31, 0x2c, 0x20, 0x32, 0x2c, 0x20, 0x33, 0x5d, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x0a, 0x01, 0x5b, 0x34, 0x2c, 0x20, 0x35, 0x2c,
    0x20, 0x36, 0x5d, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS BYTEA) AS "col" FROM (  VALUES (''), ('\x0001'),
// ('\x01020304'), ('\xFEFF'), (NULL)) AS drvd("col")) TO STDOUT
// WITH (FORMAT binary);
static const uint8_t kTestPgCopyBinary[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0xfe, 0xff, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
// COPY (SELECT CAST("col" AS TEXT) AS "col" FROM (  VALUES ('ok'), ('sad'), (NULL::text))
// AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyEnum[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x02, 0x6f, 0x6b, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
    0x73, 0x61, 0x64, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST("col" AS INTEGER ARRAY) AS "col" FROM (  VALUES ('{-123, -1}'), ('{0,
// 1, 123}'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyIntegerArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0x85, 0x00, 0x00, 0x00,
    0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x7b, 0x00,
    0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// CREATE TYPE custom_record AS (nested1 integer, nested2 double precision);
// COPY (SELECT CAST("col" AS custom_record) AS "col" FROM (  VALUES ('(123, 456.789)'),
// ('(12, 345.678)'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyCustomRecord[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
    0x00, 0x7b, 0x00, 0x00, 0x02, 0xbd, 0x00, 0x00, 0x00, 0x08, 0x40, 0x7c, 0x8c,
    0x9f, 0xbe, 0x76, 0xc8, 0xb4, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
    0x0c, 0x00, 0x00, 0x02, 0xbd, 0x00, 0x00, 0x00, 0x08, 0x40, 0x75, 0x9a, 0xd9,
    0x16, 0x87, 0x2b, 0x02, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// For full coverage, ensure that this contains NUMERIC examples that:
// - Have >= four zeroes to the left of the decimal point
// - Have >= four zeroes to the right of the decimal point
// - Include special values (nan, -inf, inf, NULL)
// - Have >= four trailing zeroes to the right of the decimal point
// - Have >= four leading zeroes before the first digit to the right of the decimal point
// - Is < 0 (negative)
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (  VALUES (1000000), ('0.00001234'),
// ('1.0000'), (-123.456), (123.456), ('nan'), ('-inf'), ('inf'), (NULL)) AS drvd(col)) TO
// STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyNumeric[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00,
    0x01, 0xff, 0xfe, 0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x01, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0c, 0x00, 0x02, 0x00, 0x00, 0x40, 0x00, 0x00, 0x03, 0x00, 0x7b, 0x11,
    0xd0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x03, 0x00, 0x7b, 0x11, 0xd0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0xf0, 0x00, 0x00, 0x20, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0xd0, 0x00, 0x00, 0x20, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// COPY (SELECT CAST(col AS NUMERIC(16, 10)) AS col FROM (  VALUES ('0'), ('1.01234'),
// ('1.0123456789'), ('-1.01234'), ('-1.0123456789'), ('nan'), (NULL)) AS
// drvd(col)) TO '/tmp/pgdata.out' WITH (FORMAT binary);
static const uint8_t kTestPgCopyNumeric16_10[] = {
    0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x00, 0x7B, 0x0F, 0xA0, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x10, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x00,
    0x7B, 0x11, 0xD7, 0x22, 0xC4, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x00, 0x03, 0x00,
    0x00, 0x40, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x00, 0x7B, 0x0F, 0xA0, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x10, 0x00, 0x04, 0x00, 0x00, 0x40, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x00,
    0x7B, 0x11, 0xD7, 0x22, 0xC4, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

// COPY (SELECT
//   CAST('' AS int2vector),
//   CAST('-32768' AS int2vector),
//   CAST('-32768 32767' AS int2vector),
//   CAST('-1 0 1 42' AS int2vector)
// ) TO '/tmp/pgdata.bin' WITH (FORMAT binary);
static const uint8_t kTestPgCopyInt2vector[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x02, 0x7f, 0xff, 0x00,
    0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x15, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x2a, 0xff, 0xff,
};

}  // namespace adbcpq
