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
    public class AdbcUsageSchema
    {
        public string? FkCatalog { get; set; }

        public string? FkDbSchema { get; set; }

        public string? FkTable { get; set; }

        public string? FkColumnName { get; set; }

        public override bool Equals(object? obj)
        {
            if (obj == null || obj.GetType() != this.GetType())
            {
                return false;
            }

            var other = (AdbcUsageSchema)obj;
            return this.FkCatalog == other.FkCatalog && this.FkDbSchema == other.FkDbSchema && this.FkTable == other.FkTable && this.FkColumnName == other.FkColumnName;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 31 + (FkCatalog != null ? FkCatalog.GetHashCode() : 0);
            hash = hash * 31 + (FkDbSchema != null ? FkDbSchema.GetHashCode() : 0);
            hash = hash * 31 + (FkTable != null ? FkTable.GetHashCode() : 0);
            hash = hash * 31 + (FkColumnName != null ? FkColumnName.GetHashCode() : 0);
            return hash;
        }
    }
}
