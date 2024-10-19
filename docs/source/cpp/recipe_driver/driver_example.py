# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# RECIPE STARTS HERE
#: After verifying the basic driver functionality, we can use the
#: ``adbc_driver_manager`` Python package's built-in dbapi implementation
#: to expose a ready-to-go Pythonic database API. This is also useful for
#: high-level testing!
#:
#: First, we'll import pathlib for a few path calculations and the
#: ``adbc_driver_manager``'s ``dbapi`` module:
from pathlib import Path

from adbc_driver_manager import dbapi
#: Next, we'll define a ``connect()`` function that wraps ``dbapi.connect()``
#: with the location of the shared library we built using ``cmake`` in the previous
#: section. For the purposes of our tutorial, this will be in the CMake ``build/``
#: directory.
def connect(uri: str):
    build_dir = Path(__file__).parent / "build"
    for lib in [
        "libdriver_example.dylib",
        "libdriver_example.so",
        "driver_example.dll",
    ]:
        driver_lib = build_dir / lib
        if driver_lib.exists():
            return dbapi.connect(
                driver=str(driver_lib.resolve()), db_kwargs={"uri": uri}
            )

    raise RuntimeError("Can't find driver shared object")
#: Next, we can give our driver a go! The two pieces we implemented in the driver
#: were the "bulk ingest" feature and "select all from", so let's see if it works!
if __name__ == "__main__":
    import pyarrow
    import os

    with connect(uri=Path(__file__).parent.as_uri()) as con:
        data = pyarrow.table({"col": [1, 2, 3]})
        with con.cursor() as cur:
            cur.adbc_ingest("example.arrows", data, mode="create")

        with con.cursor() as cur:
            cur.execute("SELECT * FROM example.arrows")
            print(cur.fetchall())

        os.unlink(Path(__file__).parent / "example.arrows")
