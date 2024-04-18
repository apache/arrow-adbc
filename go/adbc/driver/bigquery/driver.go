package bigquery

import (
	"cloud.google.com/go/bigquery"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/maps"
	"runtime/debug"
	"strings"
)

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

const (
	OptionStringAuthType               = "adbc.bigquery.sql.auth_type"
	OptionStringCredentials            = "adbc.bigquery.sql.credentials"
	OptionStringProjectID              = "adbc.bigquery.sql.project_id"
	OptionStringDatasetID              = "adbc.bigquery.sql.dataset_id"
	OptionStringTableID                = "adbc.bigquery.sql.table_id"
	OptionValueAuthTypeDefault         = "default"
	OptionValueAuthTypeCredentialsFile = "credentials_file"

	OptionStringQueryDestinationTable  = "adbc.bigquery.sql.query.destination_table"
	OptionStringQueryDefaultProjectID  = "adbc.bigquery.sql.query.default_project_id"
	OptionStringQueryDefaultDatasetID  = "adbc.bigquery.sql.query.default_dataset_id"
	OptionStringQueryCreateDisposition = "adbc.bigquery.sql.query.create_disposition"
	OptionStringQueryWriteDisposition  = "adbc.bigquery.sql.query.write_disposition"
	OptionBoolQueryDisableQueryCache   = "adbc.bigquery.sql.query.disable_query_cache"
	OptionBoolDisableFlattenedResults  = "adbc.bigquery.sql.query.disable_flattened_results"
	OptionBoolQueryAllowLargeResults   = "adbc.bigquery.sql.query.allow_large_results"
	OptionStringQueryPriority          = "adbc.bigquery.sql.query.priority"
	OptionIntQueryMaxBillingTier       = "adbc.bigquery.sql.query.max_billing_tier"
	OptionIntQueryMaxBytesBilled       = "adbc.bigquery.sql.query.max_bytes_billed"
	OptionBoolQueryUseLegacySQL        = "adbc.bigquery.sql.query.use_legacy_sql"
	OptionBoolQueryDryRun              = "adbc.bigquery.sql.query.dry_run"
	OptionBoolQueryCreateSession       = "adbc.bigquery.sql.query.create_session"
	OptionIntQueryJobTimeout           = "adbc.bigquery.sql.query.job_timeout"
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "cloud.google.com/go/bigquery":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
	alloc memory.Allocator
}

// NewDriver creates a new BigQuery driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("BigQuery")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
		alloc:          alloc,
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	opts = maps.Clone(opts)
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		alloc:            d.alloc,
		authType:         OptionValueAuthTypeDefault,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}

func stringToBool(value string) (bool, error) {
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("Invalid boolean value, expected `true` or `false`, got `%s`", value),
		}
	}
}

func stringToTable(value string) (*bigquery.Table, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("Invalid Table Reference format, expected `ProjectId.DatasetId.TableId`, got: `%s`", value),
		}
	}

	return &bigquery.Table{
		ProjectID: parts[0],
		DatasetID: parts[1],
		TableID:   parts[2],
	}, nil
}

func stringToTableCreateDisposition(value string) (bigquery.TableCreateDisposition, error) {
	switch value {
	case "CREATE_NEVER":
		return bigquery.CreateNever, nil
	case "CREATE_IF_NEEDED":
		return bigquery.CreateIfNeeded, nil
	default:
		return bigquery.CreateIfNeeded, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table create disposition value `%s`", value),
		}
	}
}

func stringToTableWriteDisposition(value string) (bigquery.TableWriteDisposition, error) {
	switch value {
	case "WRITE_APPEND":
		return bigquery.WriteAppend, nil
	case "WRITE_TRUNCATE":
		return bigquery.WriteTruncate, nil
	case "WRITE_EMPTY":
		return bigquery.WriteEmpty, nil
	default:
		return bigquery.WriteEmpty, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table write disposition value `%s`", value),
		}
	}
}

func stringToQueryPriority(value string) (bigquery.QueryPriority, error) {
	switch value {
	case "BATCH":
		return bigquery.BatchPriority, nil
	case "INTERACTIVE":
		return bigquery.InteractivePriority, nil
	default:
		return bigquery.InteractivePriority, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table create disposition value `%s`", value),
		}
	}
}

func boolToString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func tableToString(value *bigquery.Table) string {
	if value == nil {
		return ""
	} else {
		return fmt.Sprintf("%s.%s.%s", value.ProjectID, value.DatasetID, value.TableID)
	}
}

func tableCreateDispositionToString(value bigquery.TableCreateDisposition) string {
	switch value {
	case bigquery.CreateNever:
		return "CREATE_NEVER"
	case bigquery.CreateIfNeeded:
		return "CREATE_IF_NEEDED"
	}
	return ""
}

func tableWriteDispositionToString(value bigquery.TableWriteDisposition) string {
	switch value {
	case bigquery.WriteAppend:
		return "WRITE_APPEND"
	case bigquery.WriteTruncate:
		return "WRITE_TRUNCATE"
	case bigquery.WriteEmpty:
		return "WRITE_EMPTY"
	}
	return ""
}

func queryPriorityToString(value bigquery.QueryPriority) string {
	switch value {
	case bigquery.BatchPriority:
		return "BATCH"
	case bigquery.InteractivePriority:
		return "INTERACTIVE"
	}
	return ""
}
