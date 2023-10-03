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
  try {
    var database = new GADBC.Database ();
    database.set_option ("driver", "adbc_driver_sqlite");
    database.set_option ("uri", "test.sqlite");
    database.init ();
    GLib.message ("Database initialization done...");
    try {
      var conn = new GADBC.Connection ();
      conn.init (database);
      GLib.message ("Connection to database initialized...");
      try {
        var stm = new GADBC.Statement (conn);
        string sql = "SELECT sqlite_version() AS version";
        stm.set_sql_query (sql);
        void *c_abi_array_stream = null;
        stm.execute (true, out c_abi_array_stream, null);
        try {
            GLib.message ("Statement executed: %s", sql);
            var reader = GArrow.RecordBatchReader.import (c_abi_array_stream);
            var table = reader.read_all ();
            GLib.message ("Executed result: %s", table.to_string ());
        } finally {
            GLib.free (c_abi_array_stream);
        }
      GLib.message ("Statement executed: %s", sql);
      }
      catch (GLib.Error e) {
        GLib.message ("Error executing statement: %s", e.message);
      }
    }
    catch (GLib.Error e) {
      GLib.message ("Error initializing the connection: %s", e.message);
    }
  }
  catch (GLib.Error e) {
    GLib.message ("Error initializing the database: %s", e.message);
  }

  return Posix.EXIT_SUCCESS;
}
