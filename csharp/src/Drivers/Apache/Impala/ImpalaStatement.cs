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

using System;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal class ImpalaStatement : HiveServer2Statement
    {
        internal ImpalaStatement(ImpalaConnection connection)
            : base(connection)
        {
        }

        public override object GetValue(IArrowArray arrowArray, int index)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Provides the constant string key values to the <see cref="AdbcStatement.SetOption(string, string)" /> method.
        /// </summary>
        public new sealed class Options : HiveServer2Statement.Options
        {
            // options specific to Impala go here
        }
    }
}
