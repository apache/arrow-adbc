#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

### copies the Snowflake binaries for all platforms to be packaged for NuGet

echo "Copying the Snowflake ADBC Go drivers"
echo "IsPackagingPipeline=$IsPackagingPipeline"

if [[ -z "${IsPackagingPipeline}" ]]; then
    echo "IsPackagingPipeline environment variable does not exist."
    exit 0
fi

# Get the value of the IsPackagingPipeline environment variable
IsPackagingPipelineValue="${IsPackagingPipeline}"

# Check if the value is "true"
if [[ "${IsPackagingPipelineValue}" != "true" ]]; then
    echo "IsPackagingPipeline is not set to 'true'. Exiting the script."
    exit 0
fi

destination_dir=$(pwd)

file="libadbc_driver_snowflake.*"

if ls libadbc_driver_snowflake.* 1> /dev/null 2>&1; then
    echo "Files found. Exiting the script."
    exit 0
else
    cd ../../../../../go/adbc/pkg

    source_dir=$(pwd)

    files_to_copy=$(find "$source_dir" -type f -name "$file")

    for file in $files_to_copy; do
        cp "$file" "$destination_dir"
        echo "Copied $file to $destination_dir"
    done
fi
