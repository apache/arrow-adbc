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

package snowflake

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func convertArrowToNamedValue(batch arrow.Record, index int) ([]driver.NamedValue, error) {
	// see goTypeToSnowflake in gosnowflake
	// technically, snowflake can bind an array of values at once, but
	// only for INSERT, so we can't take advantage of that without
	// analyzing the query ourselves
	params := make([]driver.NamedValue, batch.NumCols())
	for i, field := range batch.Schema().Fields() {
		rawColumn := batch.Column(i)
		params[i].Ordinal = i + 1
		switch column := rawColumn.(type) {
		case *array.Boolean:
			params[i].Value = sql.NullBool{
				Bool:  column.Value(index),
				Valid: column.IsValid(index),
			}
		case *array.Float32:
			// Snowflake only recognizes float64
			params[i].Value = sql.NullFloat64{
				Float64: float64(column.Value(index)),
				Valid:   column.IsValid(index),
			}
		case *array.Float64:
			params[i].Value = sql.NullFloat64{
				Float64: column.Value(index),
				Valid:   column.IsValid(index),
			}
		case *array.Int8:
			// Snowflake only recognizes int64
			params[i].Value = sql.NullInt64{
				Int64: int64(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Int16:
			params[i].Value = sql.NullInt64{
				Int64: int64(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Int32:
			params[i].Value = sql.NullInt64{
				Int64: int64(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Int64:
			params[i].Value = sql.NullInt64{
				Int64: column.Value(index),
				Valid: column.IsValid(index),
			}
		case *array.String:
			params[i].Value = sql.NullString{
				String: column.Value(index),
				Valid:  column.IsValid(index),
			}
		case *array.LargeString:
			params[i].Value = sql.NullString{
				String: column.Value(index),
				Valid:  column.IsValid(index),
			}
		default:
			return nil, adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  fmt.Sprintf("[Snowflake] Unsupported bind param '%s' type %s", field.Name, field.Type.String()),
			}
		}
	}
	return params, nil
}

type snowflakeBindReader struct {
	doQuery      func([]driver.NamedValue) (array.RecordReader, error)
	currentBatch arrow.Record
	nextIndex    int64
	// may be nil if we bound only a batch
	stream array.RecordReader
}

func (r *snowflakeBindReader) Release() {
	if r.currentBatch != nil {
		r.currentBatch.Release()
		r.currentBatch = nil
	}
	if r.stream != nil {
		r.stream.Release()
		r.stream = nil
	}
}

func (r *snowflakeBindReader) Next() (array.RecordReader, error) {
	params, err := r.NextParams()
	if err != nil {
		// includes EOF
		return nil, err
	}
	return r.doQuery(params)
}

func (r *snowflakeBindReader) NextParams() ([]driver.NamedValue, error) {
	for r.currentBatch == nil || r.nextIndex >= r.currentBatch.NumRows() {
		// We can be used both by binding a stream or by binding a
		// batch. In the latter case, we have to release the batch,
		// but not in the former case. Unify the cases by always
		// releasing the batch, adding an "extra" retain so that the
		// release does not cause issues.
		if r.currentBatch != nil {
			r.currentBatch.Release()
		}
		r.currentBatch = nil
		if r.stream != nil && r.stream.Next() {
			r.currentBatch = r.stream.Record()
			r.currentBatch.Retain()
			r.nextIndex = 0
			continue
		} else if r.stream != nil && r.stream.Err() != nil {
			return nil, r.stream.Err()
		} else {
			// no more params
			return nil, io.EOF
		}
	}

	params, err := convertArrowToNamedValue(r.currentBatch, int(r.nextIndex))
	r.nextIndex++
	return params, err
}
