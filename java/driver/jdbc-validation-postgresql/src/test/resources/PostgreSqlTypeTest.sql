-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

DROP TABLE IF EXISTS adbc_alltypes;

CREATE TABLE adbc_alltypes (
    bigint_t BIGINT,
    blob_t BYTEA,
    boolean_t BOOLEAN,
    date_t DATE,
    int_t INT,
    smallint_t SMALLINT,
    text_t TEXT,
    time_without_time_zone_t TIME WITHOUT TIME ZONE,
    time_with_time_zone_t TIME WITH TIME ZONE,
    timestamp_without_time_zone_t TIMESTAMP WITHOUT TIME ZONE,
    timestamp_without_time_zone_p6_t TIMESTAMP (6) WITHOUT TIME ZONE,
    timestamp_without_time_zone_p5_t TIMESTAMP (5) WITHOUT TIME ZONE,
    timestamp_without_time_zone_p4_t TIMESTAMP (4) WITHOUT TIME ZONE,
    timestamp_without_time_zone_p3_t TIMESTAMP (3) WITHOUT TIME ZONE,
    timestamp_without_time_zone_p2_t TIMESTAMP (2) WITHOUT TIME ZONE,
    timestamp_without_time_zone_p1_t TIMESTAMP (1) WITHOUT TIME ZONE,
    timestamp_without_time_zone_p0_t TIMESTAMP (0) WITHOUT TIME ZONE,
    timestamp_with_time_zone_t TIMESTAMP WITH TIME ZONE
);

INSERT INTO adbc_alltypes VALUES (
    42,
    'abcd',
    true,
    '2000-01-01',
    42,
    42,
    'foo',
    '04:05:06.789012',
    '04:05:06.789012-08:00',
    TIMESTAMP '2000-01-02 03:04:05.123456',
    TIMESTAMP (6) '2000-01-02 03:04:05.123456',
    TIMESTAMP (5) '2000-01-02 03:04:05.12345',
    TIMESTAMP (4) '2000-01-02 03:04:05.1234',
    TIMESTAMP (3) '2000-01-02 03:04:05.123',
    TIMESTAMP (2) '2000-01-02 03:04:05.12',
    TIMESTAMP (1) '2000-01-02 03:04:05.1',
    TIMESTAMP (0) '2000-01-02 03:04:05',
    TIMESTAMP WITH TIME ZONE '2000-01-02 03:04:05.123456+06'
), (
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);
