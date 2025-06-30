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

FROM spiceai/spiceai:1.4.0-models AS spiceai

FROM debian:bookworm-slim

RUN apt update \
  && apt install --yes ca-certificates libssl3 curl --no-install-recommends

COPY --from=spiceai /usr/local/bin/spiced /usr/local/bin/spiced

# Create the Spicepod configuration file
COPY <<EOF /app/spicepod.yaml
version: v1
kind: Spicepod
name: app

datasets:
  - from: s3://spiceai-demo-datasets/tpch/nation/
    name: nation
    params:
      file_format: parquet
      client_timeout: 60s
    acceleration:
      enabled: true
EOF

EXPOSE 8090
EXPOSE 9090
EXPOSE 50051

WORKDIR /app

# Start the Spice runtime with specified arguments
CMD ["/usr/local/bin/spiced", "--http", "0.0.0.0:8090", "--metrics", "0.0.0.0:9090", "--flight", "0.0.0.0:50051"]
