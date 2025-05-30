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

package databricks

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

const MetadataKeyDatabricksType = "DATABRICKS_TYPE"

type commandReader struct {
	refCount int64

	cmdExecution compute.CommandExecutionInterface

	// Command Execution that this reader is associated with.
	CommandId string

	Results *compute.Results

	rec    arrow.Record
	err    error
	schema *arrow.Schema

	cancelFn context.CancelFunc
}

func NewCommandRecordReader(
	cmdExecution compute.CommandExecutionInterface, commandId string, results *compute.Results) (*commandReader, error) {
	// Convert the Databricks schema to an Arrow schema
	schema, err := ClusterModeSchemaToArrowSchema(results.Schema)
	if err != nil {
		return nil, err
	}
	r := &commandReader{
		refCount: 1,

		cmdExecution: cmdExecution,

		CommandId: commandId,
		Results:   results,

		schema: schema,
		rec:    nil,
		err:    nil,

		cancelFn: func() {},
	}

	return r, nil
}

func (r *commandReader) setRecord() {
	// For command execution, we need to convert the result to an Arrow record
	results := r.Results
	if results.ResultType == compute.ResultTypeTable {
		switch results.Data.(type) {
		case []interface{}:
			rows := results.Data.([]interface{})
			r.rec, r.err = BuildFromRows(r.schema, rows)
		default:
			r.err = adbc.Error{
				Code: adbc.StatusInvalidData,
				Msg:  fmt.Sprintf("Unexpected command result type: %T", results.Data),
			}
		}

	} else if results.ResultType == compute.ResultTypeText {
		// For text result, return an empty record with a single string column
		fields := []arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}
		schema := arrow.NewSchema(fields, nil)
		rows := results.Data.([]interface{})
		r.rec, r.err = BuildFromRows(schema, rows)
	} else {
		r.err = adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("Unexpected command result type: %s", results.ResultType),
		}
	}
}

// \post: if returns true, r.Record() != nil && r.err == nil
// \post: if returns false, r.Record() == nil and r.err *MUST* be checked
func (r *commandReader) Next() bool {

	if r.rec == nil {
		r.setRecord()
		if r.err == nil {
			return true
		}
	}
	return false
}

func (r *commandReader) Record() arrow.Record {
	return r.rec
}

func (r *commandReader) Err() error {
	return r.err
}

func (r *commandReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *commandReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		// TODO: cancel HTTP connection
		// TODO: close channel
		r.cancelFn()
	}
}

func (r *commandReader) TotalRowCount() int64 {
	if r.Results.ResultType == compute.ResultTypeTable {
		return int64(len(r.Results.Data.([]interface{})))
	}
	return 0
}

func (r *commandReader) Schema() *arrow.Schema {
	return r.schema
}
