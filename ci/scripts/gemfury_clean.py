#!/usr/bin/env python3
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

"""Remove old packages from Gemfury."""

import datetime
import os
import random
import time

import requests

BASE_URL = "https://api.fury.io/1"


def main():
    token = os.environ["GEMFURY_API_TOKEN"]
    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)

    with requests.Session() as session:
        session.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        packages = session.get(f"{BASE_URL}/packages?limit=50").json()
        for i, package in enumerate(packages):
            if i > 0:
                time.sleep(random.randint(3, 8))

            print("Cleaning", package["name"])
            versions = session.get(
                f"{BASE_URL}/packages/{package['id']}/versions?limit=50"
            ).json()
            print(versions)
            versions.sort(key=lambda v: v["created_at"], reverse=True)

            # Always keep at least 1 version
            to_delete = [
                version["id"]
                for version in versions[1:]
                if version["created_at"] < cutoff.isoformat()
            ]
            print("Removing", len(to_delete), "version(s) of", len(versions))

            for version_id in to_delete:
                session.delete(f"{BASE_URL}/versions/{version_id}").raise_for_status()
                time.sleep(random.randint(1, 3))


if __name__ == "__main__":
    main()
