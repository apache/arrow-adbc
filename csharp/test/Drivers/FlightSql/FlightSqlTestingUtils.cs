<<<<<<<< HEAD:csharp/test/Apache.Arrow.Adbc.Tests/Metadata/AdbcTable.cs
﻿/*
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

using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Tests.Metadata
{
    public class AdbcTable
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public List<AdbcColumn> Columns { get; set; }
    }
}
========
﻿/*
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

namespace Apache.Arrow.Adbc.Tests.Drivers.FlightSql
{
    internal class FlightSqlTestingUtils
    {
        internal const string FLIGHTSQL_TEST_CONFIG_VARIABLE = "FLIGHTSQL_TEST_CONFIG_FILE";
    }
}
>>>>>>>> e419d868281eb0f384f71c9915dbf0f63cfeccf6:csharp/test/Drivers/FlightSql/FlightSqlTestingUtils.cs
