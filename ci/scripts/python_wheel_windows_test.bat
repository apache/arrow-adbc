@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem   http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.

@echo on

set source_dir=%1

echo "=== (%PYTHON_VERSION%) Installing wheels ==="

FOR %%c IN (adbc_driver_bigquery adbc_driver_manager adbc_driver_flightsql adbc_driver_postgresql adbc_driver_sqlite adbc_driver_snowflake) DO (
    FOR %%w IN (%source_dir%\python\%%c\dist\*.whl) DO (
        pip install --no-deps --force-reinstall %%w || exit /B 1
    )
)

pip install importlib-resources pytest pyarrow pandas polars protobuf

echo "=== (%PYTHON_VERSION%) Testing wheels ==="

FOR %%c IN (adbc_driver_bigquery adbc_driver_manager adbc_driver_flightsql adbc_driver_postgresql adbc_driver_sqlite adbc_driver_snowflake) DO (
    echo "=== Testing %%c ==="
    python -c "import %%c" || exit /B 1
    python -c "import %%c.dbapi" || exit /B 1
    python -m pytest -vvx --import-mode append -k "not duckdb and not sqlite and not polars" %source_dir%\python\%%c\tests || exit /B 1
)
