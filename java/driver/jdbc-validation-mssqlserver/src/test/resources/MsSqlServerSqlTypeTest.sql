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
    blob_t VARBINARY(MAX),
    boolean_t BIT,
    date_t DATE,
    int_t INT,
    smallint_t SMALLINT,
    text_t TEXT,
    time_without_time_zone_t TIME,
    timestamp_without_time_zone_t DATETIME2,
    timestamp_with_time_zone_t DATETIMEOFFSET
);

INSERT INTO adbc_alltypes VALUES (
    42,
    CONVERT(VARBINARY(MAX), 'abcd'),
    1,
    '2000-01-01',
    42,
    42,
    'foo',
    '04:05:06.789012',
    '2000-01-02T03:04:05.123456',
    '2000-01-02T03:04:05.123456+06:00'
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
    NULL
);
