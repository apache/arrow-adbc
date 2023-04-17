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

package snowflake_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestSnowflake(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := snowflake.Driver{Alloc: mem}
	dsn := os.Getenv("SNOWFLAKE_URI")
	opts := map[string]string{adbc.OptionKeyURI: dsn}
	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)

	cnxn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cnxn.Close()

	rdr, err := cnxn.GetInfo(context.Background(), nil)
	require.NoError(t, err)
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.Record()
		fmt.Println(rec)
	}

	require.NoError(t, rdr.Err())

	var cat = "IBIS_TESTING"
	var schema = "MTOPOL"
	rdr, err = cnxn.GetObjects(context.Background(), adbc.ObjectDepthAll, &cat, &schema, nil, nil, []string{"BASE TABLE"})
	require.NoError(t, err)
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.Record()
		fmt.Println(rec)
	}

	require.NoError(t, rdr.Err())
}

func TestSnowflakeGetSchema(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := snowflake.Driver{Alloc: mem}
	dsn := os.Getenv("SNOWFLAKE_URI")
	opts := map[string]string{adbc.OptionKeyURI: dsn}
	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)

	cnxn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cnxn.Close()

	dbschema := "MTOPOL"
	fmt.Println(cnxn.GetTableSchema(context.Background(), nil, &dbschema, "INTTABLE"))
}
