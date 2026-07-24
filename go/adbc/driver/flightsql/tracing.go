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

package flightsql

import (
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/metadata"
)

// traceHeaderAttrsWithPrefix returns OpenTelemetry attributes for
// allow-listed metadata keys. It emits only curated correlation
// headers so callers can promote external request IDs into traces
// without leaking credentials.
func traceHeaderAttrsWithPrefix(md metadata.MD, prefix string) []attribute.KeyValue {
	if len(md) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, 4)
	for _, k := range wellKnownCorrelationHeaders {
		if vals := md.Get(k); len(vals) > 0 {
			out = append(out, attribute.Key(prefix+k).StringSlice(vals))
		}
	}
	return out
}
