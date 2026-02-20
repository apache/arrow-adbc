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

package adbc

import "context"

// databaseContextAdapter wraps a Database to implement DatabaseContext.
type databaseContextAdapter struct {
	db Database
}

// AsDatabaseContext wraps a Database to implement DatabaseContext.
// This adapter allows using a non-context Database implementation with
// context-aware code. The context parameter is used for methods that
// already accept context (like Open), but is effectively ignored for
// methods that don't (like Close, SetOptions) since the underlying
// implementation cannot respond to cancellation or deadlines.
func AsDatabaseContext(db Database) DatabaseContext {
	if db == nil {
		return nil
	}
	// If it already implements DatabaseContext, return it directly
	if dbCtx, ok := db.(DatabaseContext); ok {
		return dbCtx
	}
	return &databaseContextAdapter{db: db}
}

func (d *databaseContextAdapter) SetOptions(ctx context.Context, opts map[string]string) error {
	// Context cannot be propagated to SetOptions since it doesn't accept context
	return d.db.SetOptions(opts)
}

func (d *databaseContextAdapter) Open(ctx context.Context) (ConnectionContext, error) {
	// Pass context through since Open already accepts it
	conn, err := d.db.Open(ctx)
	if err != nil {
		return nil, err
	}
	// Wrap the returned Connection as ConnectionContext
	return AsConnectionContext(conn), nil
}

func (d *databaseContextAdapter) Close(ctx context.Context) error {
	// Context cannot be propagated to Close since it doesn't accept context
	return d.db.Close()
}
