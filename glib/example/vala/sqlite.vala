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

int main (string[] args) {
    var exit_code = Posix.EXIT_FAILURE;

    try {
        var database = new GADBC.Database ();
        database.set_option ("driver", "adbc_driver_sqlite");
        database.set_option ("uri", ":memory:");
        database.init ();
        try {
            var connection = new GADBC.Connection ();
            connection.init (database);
            try {
                var statement = new GADBC.Statement (connection);
                string sql = "SELECT sqlite_version() AS version";
                statement.set_sql_query (sql);
                try {
                    void *c_abi_array_stream = null;
                    int64 n_rows_affected;
                    statement.execute (true, out c_abi_array_stream, out n_rows_affected);
                    try {
                        var reader = GArrow.RecordBatchReader.import (c_abi_array_stream);
                        var table = reader.read_all ();
                        stdout.printf ("Result:\n%s", table.to_string ());
                    } finally {
                        GLib.free (c_abi_array_stream);
                    }
                    exit_code = Posix.EXIT_SUCCESS;
                } catch (GLib.Error error) {
                    GLib.error ("Failed to execute a statement: %s", error.message);
                }
            }
            catch (GLib.Error error) {
                GLib.error ("Failed to create a statement: %s", error.message);
            }
        }
        catch (GLib.Error error) {
            GLib.error ("Failed to create a connection: %s", error.message);
        }
    }
    catch (GLib.Error error) {
        GLib.error ("Failed to create a database: %s", error.message);
    }

    return exit_code;
}
