-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

local lgi = require("lgi")
local GADBC = lgi.ADBC
local GADBCArrow = lgi.ADBCArrow

local database = GADBC.Database.new()
local success, err = database:set_option("driver", "adbc_driver_sqlite")
if not success then
	error("Failed to set driver: " .. err.message)
end
success, err = database:set_option("uri", ":memory:")
if not success then
	error("Failed to set database URI: " .. err.message)
end
success, err = database:init()
if not success then
	error("Failed to create a database: " .. err.message)
end

local connection = GADBC.Connection.new()
success, err = connection:init(database)
if not success then
	error("Failed to create a connection: " .. err.message)
end

local statement = GADBCArrow.Statement.new(connection)
local sql = "SELECT sqlite_version() AS version"
success, err = statement:set_sql_query(sql)
if not success then
	error("Failed to set a query: " .. err.message)
end

local reader, err_or_n_rows_affected = statement:execute(true)
if not reader then
	error("Failed to execute a statement: " .. err_or_n_rows_affected.message)
end

local result_table = reader:read_all()
io.write(string.format("Result:\n%s", result_table:to_string()))
