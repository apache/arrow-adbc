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

namespace Apache.Arrow.Adbc.Tests.Metadata
{
    public class AdbcColumn
    {
        public string name { get; set; }

        public int? ordinal_position { get; set; }

        public string remarks { get; set; }

        public short? xdbc_data_type { get; set; }

        public string xdbc_type_name { get; set; }

        public int? xdbc_column_size { get; set; }

        public short? xdbc_decimal_digits { get; set; }

        public short? xdbc_num_prec_radix { get; set; }

        public short? xdbc_nullable { get; set; }

        public string xdbc_column_def { get; set; }

        public short? xdbc_sql_data_type { get; set; }

        public short? xdbc_datetime_sub { get; set; }

        public int? xdbc_char_octet_length { get; set; }

        public string xdbc_is_nullable { get; set; }

        public string xdbc_scope_catalog { get; set; }

        public string xdbc_scope_schema { get; set; }

        public string xdbc_scope_table { get; set; }

        public bool? xdbc_is_autoincrement { get; set; }

        public bool? xdbc_is_generatedcolumn { get; set; }

    }
}
