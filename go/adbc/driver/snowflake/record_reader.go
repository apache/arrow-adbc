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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	MetadataKeySnowflakeType    = "SNOWFLAKE_TYPE"
	MetadataKeySnowflakeQueryID = "SNOWFLAKE_QUERY_ID"
)

func identCol(_ context.Context, a arrow.Array) (arrow.Array, error) {
	a.Retain()
	return a, nil
}

type recordTransformer = func(context.Context, arrow.RecordBatch) (arrow.RecordBatch, error)
type colTransformer = func(context.Context, arrow.Array) (arrow.Array, error)

func getRecTransformer(sc *arrow.Schema, tr []colTransformer) recordTransformer {
	return func(ctx context.Context, r arrow.RecordBatch) (arrow.RecordBatch, error) {
		if len(tr) != int(r.NumCols()) {
			return nil, adbc.Error{
				Msg:  "mismatch in record cols and transformers",
				Code: adbc.StatusInvalidState,
			}
		}

		var (
			err  error
			cols = make([]arrow.Array, r.NumCols())
		)
		for i, col := range r.Columns() {
			if cols[i], err = tr[i](ctx, col); err != nil {
				return nil, errToAdbcErr(adbc.StatusInternal, err)
			}
			defer cols[i].Release()
		}

		return array.NewRecordBatch(sc, cols, r.NumRows()), nil
	}
}

func getTransformer(sc *arrow.Schema, ld gosnowflake.ArrowStreamLoader, useHighPrecision bool, maxTimestampPrecision MaxTimestampPrecision) (*arrow.Schema, recordTransformer) {
	types := ld.RowTypes()

	fields := make([]arrow.Field, len(sc.Fields()))
	transformers := make([]func(context.Context, arrow.Array) (arrow.Array, error), len(sc.Fields()))
	for i, f := range sc.Fields() {
		srcMeta := types[i]
		originalArrowUnit := arrow.TimeUnit(srcMeta.Scale / 3)
		switch strings.ToUpper(srcMeta.Type) {
		case "FIXED":
			switch f.Type.ID() {
			case arrow.DECIMAL, arrow.DECIMAL256:
				if useHighPrecision {
					transformers[i] = identCol
				} else {
					if srcMeta.Scale == 0 {
						f.Type = arrow.PrimitiveTypes.Int64
					} else {
						f.Type = arrow.PrimitiveTypes.Float64
					}
					dt := f.Type
					transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
						return compute.CastArray(ctx, a, compute.UnsafeCastOptions(dt))
					}
				}
			default:
				if useHighPrecision {
					dt := &arrow.Decimal128Type{
						Precision: int32(srcMeta.Precision),
						Scale:     int32(srcMeta.Scale),
					}
					f.Type = dt
					transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
						return integerToDecimal128(ctx, a, dt)
					}
				} else {
					if srcMeta.Scale != 0 {
						f.Type = arrow.PrimitiveTypes.Float64
						// For precisions of 16, 17 and 18, a conversion from int64 to float64 fails with an error
						// So for these precisions, we instead convert first to a decimal128 and then to a float64.
						if srcMeta.Precision > 15 && srcMeta.Precision < 19 {
							transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
								result, err := integerToDecimal128(ctx, a, &arrow.Decimal128Type{
									Precision: int32(srcMeta.Precision),
									Scale:     int32(srcMeta.Scale),
								})
								if err != nil {
									return nil, err
								}
								defer result.Release()
								return compute.CastArray(ctx, result, compute.UnsafeCastOptions(f.Type))
							}
						} else {
							// For precisions less than 16, we can simply scale the integer value appropriately
							transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
								result, err := compute.Divide(ctx, compute.ArithmeticOptions{NoCheckOverflow: true},
									&compute.ArrayDatum{Value: a.Data()},
									compute.NewDatum(math.Pow10(int(srcMeta.Scale))))
								if err != nil {
									return nil, err
								}
								defer result.Release()
								return result.(*compute.ArrayDatum).MakeArray(), nil
							}
						}
					} else {
						f.Type = arrow.PrimitiveTypes.Int64
						transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
							return compute.CastArray(ctx, a, compute.SafeCastOptions(arrow.PrimitiveTypes.Int64))
						}
					}
				}
			}
		case "TIME":
			var dt arrow.DataType
			if srcMeta.Scale < 6 {
				dt = &arrow.Time32Type{Unit: arrow.TimeUnit(srcMeta.Scale / 3)}
			} else {
				dt = &arrow.Time64Type{Unit: arrow.TimeUnit(srcMeta.Scale / 3)}
			}
			f.Type = dt
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
				return compute.CastArray(ctx, a, compute.SafeCastOptions(dt))
			}
		case "TIMESTAMP_NTZ":
			dt := &arrow.TimestampType{Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
			f.Type = dt
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {

				if a.DataType().ID() != arrow.STRUCT {
					return compute.CastArray(ctx, a, compute.SafeCastOptions(dt))
				}

				pool := compute.GetAllocator(ctx)
				tb := array.NewTimestampBuilder(pool, dt)
				defer tb.Release()

				structData := a.(*array.Struct)
				epoch := structData.Field(0).(*array.Int64).Int64Values()
				fraction := structData.Field(1).(*array.Int32).Int32Values()
				for i := 0; i < a.Len(); i++ {
					if a.IsNull(i) {
						tb.AppendNull()
						continue
					}

					// Convert fraction to nanoseconds based on the scale
					nanoseconds := int64(fraction[i]) * int64(math.Pow10(9-int(srcMeta.Scale)))
					v, err := getArrowTimestampFromTime(time.Unix(epoch[i], nanoseconds), dt.TimeUnit(), originalArrowUnit, maxTimestampPrecision)
					if err != nil {
						return nil, err
					}
					tb.Append(v)
				}
				return tb.NewArray(), nil
			}
		case "TIMESTAMP_LTZ":
			dt := &arrow.TimestampType{Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision), TimeZone: "UTC"}
			f.Type = dt
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
				pool := compute.GetAllocator(ctx)
				tb := array.NewTimestampBuilder(pool, dt)
				defer tb.Release()

				if a.DataType().ID() == arrow.STRUCT {
					structData := a.(*array.Struct)
					epoch := structData.Field(0).(*array.Int64).Int64Values()
					fraction := structData.Field(1).(*array.Int32).Int32Values()
					for i := 0; i < a.Len(); i++ {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						// Convert fraction to nanoseconds based on the scale
						nanoseconds := int64(fraction[i]) * int64(math.Pow10(9-int(srcMeta.Scale)))
						v, err := getArrowTimestampFromTime(time.Unix(epoch[i], nanoseconds), dt.TimeUnit(), originalArrowUnit, maxTimestampPrecision)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				} else {
					for i, t := range a.(*array.Int64).Int64Values() {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						tb.Append(arrow.Timestamp(t))
					}
				}
				return tb.NewArray(), nil
			}
		case "TIMESTAMP_TZ":
			// we convert each value to UTC since we have timezone information
			// with the data that lets us do so.
			dt := &arrow.TimestampType{TimeZone: "UTC", Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
			f.Type = dt
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
				pool := compute.GetAllocator(ctx)
				tb := array.NewTimestampBuilder(pool, dt)
				defer tb.Release()

				structData := a.(*array.Struct)
				if structData.NumField() == 2 {
					epoch := structData.Field(0).(*array.Int64).Int64Values()
					tzoffset := structData.Field(1).(*array.Int32).Int32Values()
					for i := 0; i < a.Len(); i++ {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						loc := gosnowflake.Location(int(tzoffset[i]) - 1440)
						// When there's no fraction field, epoch contains the timestamp in the scale's unit
						// For scale 0-2: seconds, scale 3-5: milliseconds, scale 6-8: microseconds, scale 9: nanoseconds
						var t time.Time
						switch srcMeta.Scale {
						case 0, 1, 2:
							t = time.Unix(epoch[i], 0).In(loc)
						case 3, 4, 5:
							t = time.UnixMilli(epoch[i]).In(loc)
						case 6, 7, 8:
							t = time.UnixMicro(epoch[i]).In(loc)
						case 9:
							t = time.Unix(0, epoch[i]).In(loc)
						default:
							t = time.Unix(epoch[i], 0).In(loc)
						}
						v, err := getArrowTimestampFromTime(t, dt.Unit, originalArrowUnit, maxTimestampPrecision)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				} else {
					epoch := structData.Field(0).(*array.Int64).Int64Values()
					fraction := structData.Field(1).(*array.Int32).Int32Values()
					tzoffset := structData.Field(2).(*array.Int32).Int32Values()
					for i := 0; i < a.Len(); i++ {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						// Convert fraction to nanoseconds based on the scale
						nanoseconds := int64(fraction[i]) * int64(math.Pow10(9-int(srcMeta.Scale)))
						loc := gosnowflake.Location(int(tzoffset[i]) - 1440)
						v, err := getArrowTimestampFromTime(time.Unix(epoch[i], nanoseconds).In(loc), dt.Unit, originalArrowUnit, maxTimestampPrecision)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				}
				return tb.NewArray(), nil
			}
		default:
			transformers[i] = identCol
		}

		fields[i] = f
	}

	meta := sc.Metadata().ToMap()
	if _, ok := meta[MetadataKeySnowflakeQueryID]; !ok {
		meta[MetadataKeySnowflakeQueryID] = ld.QueryID()
	}

	finalMeta := arrow.MetadataFrom(meta)
	out := arrow.NewSchema(fields, &finalMeta)

	return out, getRecTransformer(out, transformers)
}

func getArrowTimeUnit(scale int64, maxTimestampPrecision MaxTimestampPrecision) arrow.TimeUnit {
	if scale == 9 && maxTimestampPrecision == Microseconds {
		return arrow.Microsecond
	} else {
		return arrow.TimeUnit(scale / 3)
	}
}

func getArrowTimestampFromTime(val time.Time, unit arrow.TimeUnit, originalArrowUnit arrow.TimeUnit, maxTimestampPrecision MaxTimestampPrecision) (arrow.Timestamp, error) {
	if maxTimestampPrecision == NanosecondsNoOverflow && originalArrowUnit == arrow.Nanosecond {
		sec := float64(val.Unix())
		maxSeconds := math.MaxInt64 / 1e9
		minSeconds := math.MinInt64 / 1e9
		if sec > maxSeconds || sec < minSeconds {
			return 0, errToAdbcErr(adbc.StatusInvalidData, fmt.Errorf("timestamp %v overflows when converted to nanoseconds", val))
		}
	}

	if maxTimestampPrecision == Microseconds && originalArrowUnit == arrow.Nanosecond {
		return arrow.TimestampFromTime(time.UnixMicro(val.UnixMicro()), arrow.Microsecond)
	}

	return arrow.TimestampFromTime(val, unit)
}

func integerToDecimal128(ctx context.Context, a arrow.Array, dt *arrow.Decimal128Type) (arrow.Array, error) {
	// We can't do a cast directly into the destination type because the numbers we get from Snowflake
	// are scaled integers. So not only would the cast produce the wrong value, it also risks producing
	// an error of precisions which e.g. can't hold every int64. To work around these problems, we instead
	// cast into a decimal type of a precision and scale which we know will hold all values and won't
	// require scaling, We then substitute the type on this array with the actual return type.

	dt0 := &arrow.Decimal128Type{
		Precision: int32(20),
		Scale:     int32(0),
	}
	result, err := compute.CastArray(ctx, a, compute.SafeCastOptions(dt0))
	if err != nil {
		return nil, err
	}

	data := result.Data()
	result.Data().Reset(dt, data.Len(), data.Buffers(), data.Children(), data.NullN(), data.Offset())
	return result, err
}

func rowTypesToArrowSchema(_ context.Context, ld gosnowflake.ArrowStreamLoader, useHighPrecision bool, maxTimestampPrecision MaxTimestampPrecision) (*arrow.Schema, error) {
	var loc *time.Location

	metadata := ld.RowTypes()
	fields := make([]arrow.Field, len(metadata))
	for i, srcMeta := range metadata {
		fields[i] = arrow.Field{
			Name:     srcMeta.Name,
			Nullable: srcMeta.Nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				MetadataKeySnowflakeType: srcMeta.Type,
			}),
		}
		switch srcMeta.Type {
		case "fixed":
			if useHighPrecision {
				fields[i].Type = &arrow.Decimal128Type{
					Precision: int32(srcMeta.Precision),
					Scale:     int32(srcMeta.Scale),
				}
			} else {
				// Check scale to determine if this is an integer or decimal
				if srcMeta.Scale == 0 {
					fields[i].Type = arrow.PrimitiveTypes.Int64
				} else {
					fields[i].Type = arrow.PrimitiveTypes.Float64
				}
			}
		case "boolean":
			fields[i].Type = arrow.FixedWidthTypes.Boolean
		case "real":
			fields[i].Type = arrow.PrimitiveTypes.Float64
		case "date":
			fields[i].Type = arrow.PrimitiveTypes.Date32
		case "time":
			fields[i].Type = arrow.FixedWidthTypes.Time64ns
		case "timestamp_ntz":
			fields[i].Type = &arrow.TimestampType{Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
		case "timestamp_tz":
			fields[i].Type = &arrow.TimestampType{TimeZone: "UTC", Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
		case "timestamp_ltz":
			if loc == nil {
				loc = ld.Location()
			}
			if maxTimestampPrecision == Microseconds {
				fields[i].Type = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()}
			} else {
				fields[i].Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
			}
		case "binary":
			fields[i].Type = arrow.BinaryTypes.Binary
		default:
			fields[i].Type = arrow.BinaryTypes.String
		}
	}
	// Add query ID to schema metadata
	schemaMetadata := arrow.MetadataFrom(map[string]string{
		MetadataKeySnowflakeQueryID: ld.QueryID(),
	})
	return arrow.NewSchema(fields, &schemaMetadata), nil
}

func extractTimestamp(src *string) (sec, nsec int64, err error) {
	s, ms, hasFraction := strings.Cut(*src, ".")
	sec, err = strconv.ParseInt(s, 10, 64)
	if err != nil {
		return
	}

	if !hasFraction {
		return
	}

	nsec, err = strconv.ParseInt(ms+strings.Repeat("0", 9-len(ms)), 10, 64)
	return
}

func jsonDataToArrow(_ context.Context, bldr *array.RecordBuilder, rawData [][]*string, maxTimestampPrecision MaxTimestampPrecision) (arrow.RecordBatch, error) {
	fieldBuilders := bldr.Fields()
	for _, rec := range rawData {
		for i, col := range rec {
			field := fieldBuilders[i]

			if col == nil {
				field.AppendNull()
				continue
			}

			switch fb := field.(type) {
			case *array.Time64Builder:
				sec, nsec, err := extractTimestamp(col)
				if err != nil {
					return nil, err
				}

				fb.Append(arrow.Time64(sec*1e9 + nsec))
			case *array.TimestampBuilder:
				snowflakeType, ok := bldr.Schema().Field(i).Metadata.GetValue(MetadataKeySnowflakeType)
				if !ok {
					return nil, errToAdbcErr(
						adbc.StatusInvalidData,
						fmt.Errorf("key %s not found in metadata for field %s", MetadataKeySnowflakeType, bldr.Schema().Field(i).Name),
					)
				}

				if snowflakeType == "timestamp_tz" {
					// "timestamp_tz" should be value + offset separated by space
					tm := strings.Split(*col, " ")
					if len(tm) != 2 {
						return nil, adbc.Error{
							Msg:        "invalid TIMESTAMP_TZ data. value doesn't consist of two numeric values separated by a space: " + *col,
							SqlState:   [5]byte{'2', '2', '0', '0', '7'},
							VendorCode: 268000,
							Code:       adbc.StatusInvalidData,
						}
					}

					sec, nsec, err := extractTimestamp(&tm[0])
					if err != nil {
						return nil, err
					}
					offset, err := strconv.ParseInt(tm[1], 10, 64)
					if err != nil {
						return nil, adbc.Error{
							Msg:        "invalid TIMESTAMP_TZ data. offset value is not an integer: " + tm[1],
							SqlState:   [5]byte{'2', '2', '0', '0', '7'},
							VendorCode: 268000,
							Code:       adbc.StatusInvalidData,
						}
					}

					loc := gosnowflake.Location(int(offset) - 1440)
					tt := time.Unix(sec, nsec).In(loc)

					var unit arrow.TimeUnit
					originalArrowUnit := arrow.Nanosecond
					if maxTimestampPrecision == Microseconds {
						unit = arrow.Microsecond
					} else {
						unit = arrow.Nanosecond
					}

					ts, err := getArrowTimestampFromTime(tt, unit, originalArrowUnit, maxTimestampPrecision)
					if err != nil {
						return nil, err
					}
					fb.Append(ts)
					break
				}

				// otherwise timestamp_ntz or timestamp_ltz, which have the same physical representation
				sec, nsec, err := extractTimestamp(col)
				if err != nil {
					return nil, err
				}

				if maxTimestampPrecision == Microseconds {
					tt := time.Unix(sec, nsec)
					ts, err := getArrowTimestampFromTime(tt, arrow.Microsecond, arrow.Nanosecond, maxTimestampPrecision)
					if err != nil {
						return nil, err
					}
					fb.Append(ts)
				} else {
					fb.Append(arrow.Timestamp(sec*1e9 + nsec))
				}
			case *array.BinaryBuilder:
				b, err := hex.DecodeString(*col)
				if err != nil {
					return nil, adbc.Error{
						Msg:        err.Error(),
						VendorCode: 268002,
						SqlState:   [5]byte{'2', '2', '0', '0', '3'},
						Code:       adbc.StatusInvalidData,
					}
				}
				fb.Append(b)
			default:
				if err := fb.AppendValueFromString(*col); err != nil {
					return nil, err
				}
			}
		}
	}
	return bldr.NewRecordBatch(), nil
}

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	err        error

	cancelFn context.CancelFunc
}

const defaultStreamMaxRetries = 3

// batchStreamer is the subset of gosnowflake.ArrowStreamBatch needed for reading.
type batchStreamer interface {
	GetStream(ctx context.Context) (io.ReadCloser, error)
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read.
// Used for diagnosing truncated Arrow IPC streams.
type countingReadCloser struct {
	inner     io.ReadCloser
	bytesRead int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	c.bytesRead += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.inner.Close()
}

// readBatchRecords reads all Arrow records from a Snowflake batch with retries.
// It does not buffer the raw byte stream; instead it reads Arrow IPC messages
// directly from the network and accumulates transformed record batches in memory.
// If a stream fails mid-read, all partial records are released and the entire
// batch is re-downloaded. Records are only returned on full success, preventing
// partial results from entering channels.
func readBatchRecords(ctx context.Context, batch batchStreamer, alloc memory.Allocator, transform recordTransformer, maxRetries int) ([]arrow.RecordBatch, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		recs, err := tryReadBatch(ctx, batch, alloc, transform)
		if err == nil {
			trace.SpanFromContext(ctx).AddEvent("readBatchRecords.success", trace.WithAttributes(
				attribute.Int("attempt", attempt),
				attribute.Int("records", len(recs)),
			))
			return recs, nil
		}
		trace.SpanFromContext(ctx).AddEvent("readBatchRecords.failed", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
		// Release any partial records from the failed attempt
		for _, r := range recs {
			r.Release()
		}
		lastErr = err
	}
	return nil, fmt.Errorf("failed to read Arrow batch after %d attempts: %w", maxRetries+1, lastErr)
}

func tryReadBatch(ctx context.Context, batch batchStreamer, alloc memory.Allocator, transform recordTransformer) (recs []arrow.RecordBatch, err error) {
	raw, err := batch.GetStream(ctx)
	if err != nil {
		return nil, err
	}
	stream := &countingReadCloser{inner: raw}
	defer func() {
		err = errors.Join(err, stream.Close())
	}()

	rr, err := ipc.NewReader(stream, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("ipc.NewReader failed after reading %d bytes: %w", stream.bytesRead, err)
	}
	defer rr.Release()

	for rr.Next() && ctx.Err() == nil {
		rec := rr.RecordBatch()
		rec, err = transform(ctx, rec)
		if err != nil {
			return recs, err
		}
		recs = append(recs, rec)
	}
	if err = rr.Err(); err != nil {
		return recs, err
	}
	if ctx.Err() != nil {
		return recs, ctx.Err()
	}
	return recs, nil
}

// readJSONBatches handles the case where GetBatches() returned downloadable
// chunks that contain JSON data instead of Arrow IPC. This happens with some
// Snowflake stored procedures that return JSON-format results even when Arrow
// was requested, and where the inline JSONData() is empty because all data
// arrives via downloadable chunks.
//
// batch0Data contains the already-read bytes for batches[0] (since its stream
// was consumed during format detection). Remaining batches are read via GetStream.
func readJSONBatches(ctx context.Context, alloc memory.Allocator, ld gosnowflake.ArrowStreamLoader, batches []gosnowflake.ArrowStreamBatch, batch0Data []byte, maxTimestampPrecision MaxTimestampPrecision, useHighPrecision bool) (array.RecordReader, error) {
	trace.SpanFromContext(ctx).AddEvent("readJSONBatches", trace.WithAttributes(
		attribute.Int("batches", len(batches)),
	))

	schema, err := rowTypesToArrowSchema(ctx, ld, useHighPrecision, maxTimestampPrecision)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInternal,
		}
	}

	if ld.TotalRows() == 0 {
		return array.NewRecordReader(schema, []arrow.RecordBatch{})
	}

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	var results []arrow.RecordBatch
	var rawData [][]*string
	for batchIdx, b := range batches {
		var data []byte
		if batchIdx == 0 {
			// Use the already-read data for batch[0]
			data = batch0Data
		} else {
			rdr, err := b.GetStream(ctx)
			if err != nil {
				return nil, adbc.Error{
					Msg:  fmt.Sprintf("batch[%d]: GetStream failed: %s", batchIdx, err.Error()),
					Code: adbc.StatusInternal,
				}
			}

			data, err = io.ReadAll(rdr)
			rdrErr := rdr.Close()
			if err != nil {
				return nil, adbc.Error{
					Msg:  fmt.Sprintf("batch[%d]: ReadAll failed: %s", batchIdx, err.Error()),
					Code: adbc.StatusInternal,
				}
			} else if rdrErr != nil {
				return nil, rdrErr
			}
		}

		trace.SpanFromContext(ctx).AddEvent("readJSONBatches.batch", trace.WithAttributes(
			attribute.Int("batchIndex", batchIdx),
			attribute.Int("bytes", len(data)),
			attribute.Int64("numRows", b.NumRows()),
		))

		if cap(rawData) >= int(b.NumRows()) {
			rawData = rawData[:b.NumRows()]
		} else {
			rawData = make([][]*string, b.NumRows())
		}
		bldr.Reserve(int(b.NumRows()))

		offset, buf := int64(0), bytes.NewReader(data)
		for i := 0; i < int(b.NumRows()); i++ {
			dec := json.NewDecoder(buf)
			if err = dec.Decode(&rawData[i]); err != nil {
				return nil, adbc.Error{
					Msg:  fmt.Sprintf("batch[%d] row[%d]: JSON decode failed: %s", batchIdx, i, err.Error()),
					Code: adbc.StatusInternal,
				}
			}

			offset += dec.InputOffset() + 1
			if _, err = buf.Seek(offset, 0); err != nil {
				return nil, adbc.Error{
					Msg:  fmt.Sprintf("batch[%d] row[%d]: seek failed: %s", batchIdx, i, err.Error()),
					Code: adbc.StatusInternal,
				}
			}
		}

		rec, err := jsonDataToArrow(ctx, bldr, rawData, maxTimestampPrecision)
		if err != nil {
			return nil, err
		}
		defer rec.Release()

		results = append(results, rec)
	}

	return array.NewRecordReader(schema, results)
}

func newRecordReader(ctx context.Context, alloc memory.Allocator, ld gosnowflake.ArrowStreamLoader, bufferSize, prefetchConcurrency int, useHighPrecision, streamRetryEnabled bool, maxTimestampPrecision MaxTimestampPrecision) (array.RecordReader, error) {
	batches, err := ld.GetBatches()
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err)
	}

	// if the first chunk was JSON, that means this was a metadata query which
	// is only returning JSON data rather than Arrow
	rawData := ld.JSONData()
	if len(rawData) > 0 {
		// construct an Arrow schema based on reading the JSON metadata description of the
		// result type schema
		schema, err := rowTypesToArrowSchema(ctx, ld, useHighPrecision, maxTimestampPrecision)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInternal,
			}
		}

		if ld.TotalRows() == 0 {
			return array.NewRecordReader(schema, []arrow.RecordBatch{})
		}

		bldr := array.NewRecordBuilder(alloc, schema)
		defer bldr.Release()

		rec, err := jsonDataToArrow(ctx, bldr, rawData, maxTimestampPrecision)
		if err != nil {
			return nil, err
		}
		defer rec.Release()

		results := []arrow.RecordBatch{rec}
		for _, b := range batches {
			rdr, err := b.GetStream(ctx)
			if err != nil {
				return nil, adbc.Error{
					Msg:  err.Error(),
					Code: adbc.StatusInternal,
				}
			}

			// the "JSON" data returned isn't valid JSON. Instead it is a list of
			// comma-delimited JSON lists containing every value as a string, except
			// for a JSON null to represent nulls. Thus we can't just use the existing
			// JSON parsing code in Arrow.
			data, err := io.ReadAll(rdr)
			rdrErr := rdr.Close()
			if err != nil {
				return nil, adbc.Error{
					Msg:  err.Error(),
					Code: adbc.StatusInternal,
				}
			} else if rdrErr != nil {
				return nil, rdrErr
			}

			if cap(rawData) >= int(b.NumRows()) {
				rawData = rawData[:b.NumRows()]
			} else {
				rawData = make([][]*string, b.NumRows())
			}
			bldr.Reserve(int(b.NumRows()))

			// we grab the entire JSON message and create a bytes reader
			offset, buf := int64(0), bytes.NewReader(data)
			for i := 0; i < int(b.NumRows()); i++ {
				// we construct a decoder from the bytes.Reader to read the next JSON list
				// of columns (one row) from the input
				dec := json.NewDecoder(buf)
				if err = dec.Decode(&rawData[i]); err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}

				// dec.InputOffset() now represents the index of the ',' so we skip the comma
				offset += dec.InputOffset() + 1
				// then seek the buffer to that spot. we have to seek based on the start
				// because json.Decoder can read from the buffer more than is necessary to
				// process the JSON data.
				if _, err = buf.Seek(offset, 0); err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}
			}

			// now that we have our [][]*string of JSON data, we can pass it to get converted
			// to an Arrow record batch and appended to our slice of batches
			rec, err := jsonDataToArrow(ctx, bldr, rawData, maxTimestampPrecision)
			if err != nil {
				return nil, err
			}
			defer rec.Release()

			results = append(results, rec)
		}

		return array.NewRecordReader(schema, results)
	}

	ch := make(chan arrow.RecordBatch, bufferSize)
	group, ctx := errgroup.WithContext(compute.WithAllocator(ctx, alloc))
	ctx, cancelFn := context.WithCancel(ctx)
	group.SetLimit(prefetchConcurrency)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	chs := make([]chan arrow.RecordBatch, len(batches))
	rdr := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
	}

	if len(batches) == 0 {
		schema, err := rowTypesToArrowSchema(ctx, ld, useHighPrecision, maxTimestampPrecision)
		if err != nil {
			return nil, err
		}
		rdr.schema, _ = getTransformer(schema, ld, useHighPrecision, maxTimestampPrecision)
		return rdr, nil
	}

	trace.SpanFromContext(ctx).AddEvent("newRecordReader", trace.WithAttributes(
		attribute.Int("batches", len(batches)),
		attribute.Int64("totalRows", ld.TotalRows()),
		attribute.Int("jsonDataLen", len(ld.JSONData())),
	))

	raw0, err := batches[0].GetStream(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	// Use the QueryResultFormatProvider interface to detect when the server
	// returned JSON-formatted chunks instead of Arrow IPC. Some statements
	// such as `CALL stored_procedure() RETURNS TABLE(...)` always come back
	// as JSON regardless of the Arrow request.
	if fp, ok := ld.(gosnowflake.QueryResultFormatProvider); ok && fp.QueryResultFormat() == "json" {
		raw0Data, err := io.ReadAll(raw0)
		_ = raw0.Close()
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, fmt.Errorf("batch[0]: ReadAll failed: %w", err))
		}
		trace.SpanFromContext(ctx).AddEvent("newRecordReader.jsonFormat", trace.WithAttributes(
			attribute.Int("batch0Bytes", len(raw0Data)),
		))
		return readJSONBatches(ctx, alloc, ld, batches, raw0Data, maxTimestampPrecision, useHighPrecision)
	}

	r := &countingReadCloser{inner: raw0}

	rr, err := ipc.NewReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		trace.SpanFromContext(ctx).AddEvent("newRecordReader.ipcReaderFailed", trace.WithAttributes(
			attribute.Int64("bytesRead", r.bytesRead),
			attribute.String("error", err.Error()),
		))
		_ = r.Close()
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("batch[0]: ipc.NewReader failed after reading %d bytes: %s", r.bytesRead, err.Error()),
			Code: adbc.StatusInvalidState,
		}
	}
	trace.SpanFromContext(ctx).AddEvent("newRecordReader.schemaOK", trace.WithAttributes(
		attribute.Int64("bytesRead", r.bytesRead),
		attribute.String("schema", rr.Schema().String()),
	))

	var recTransform recordTransformer
	rdr.schema, recTransform = getTransformer(rr.Schema(), ld, useHighPrecision, maxTimestampPrecision)

	group.Go(func() (err error) {
		defer rr.Release()
		defer func() {
			err = errors.Join(err, r.Close())
		}()
		if len(batches) > 1 {
			defer close(ch)
		}

		for rr.Next() && ctx.Err() == nil {
			rec := rr.RecordBatch()
			rec, err = recTransform(ctx, rec)
			if err != nil {
				return err
			}
			ch <- rec
		}
		return rr.Err()
	})

	chs[0] = ch

	lastChannelIndex := len(chs) - 1
	go func() {
		for i := range batches[1:] {
			batch, batchIdx := &batches[i+1], i+1
			chs[batchIdx] = make(chan arrow.RecordBatch, bufferSize)
			group.Go(func(batch batchStreamer, batchIdx int) func() error {
				return func() (err error) {
					// close channels (except the last) so that Next can move on to the next channel properly
					if batchIdx != lastChannelIndex {
						defer close(chs[batchIdx])
					}

					if streamRetryEnabled {
						recs, err := readBatchRecords(ctx, batch, alloc, recTransform, defaultStreamMaxRetries)
						if err != nil {
							trace.SpanFromContext(ctx).AddEvent("batch.readBatchRecords.failed", trace.WithAttributes(
								attribute.Int("batchIndex", batchIdx),
								attribute.String("error", err.Error()),
							))
							return err
						}
						trace.SpanFromContext(ctx).AddEvent("batch.readBatchRecords.success", trace.WithAttributes(
							attribute.Int("batchIndex", batchIdx),
							attribute.Int("records", len(recs)),
						))
						for i, rec := range recs {
							select {
							case chs[batchIdx] <- rec:
								recs[i] = nil // ownership transferred to channel
							case <-ctx.Done():
								rec.Release()
								// Release only unsent records
								for _, r := range recs[i+1:] {
									if r != nil {
										r.Release()
									}
								}
								return ctx.Err()
							}
						}
						return nil
					}

					// Original streaming path: read directly from stream without buffering
					rawStream, err := batch.GetStream(ctx)
					if err != nil {
						trace.SpanFromContext(ctx).AddEvent("batch.GetStream.failed", trace.WithAttributes(
							attribute.Int("batchIndex", batchIdx),
							attribute.String("error", err.Error()),
						))
						return err
					}
					countingStream := &countingReadCloser{inner: rawStream}
					defer func() {
						err = errors.Join(err, countingStream.Close())
					}()

					rr, err := ipc.NewReader(countingStream, ipc.WithAllocator(alloc))
					if err != nil {
						trace.SpanFromContext(ctx).AddEvent("batch.ipcReader.failed", trace.WithAttributes(
							attribute.Int("batchIndex", batchIdx),
							attribute.Int64("bytesRead", countingStream.bytesRead),
							attribute.String("error", err.Error()),
						))
						return fmt.Errorf("batch[%d]: ipc.NewReader failed after reading %d bytes: %w", batchIdx, countingStream.bytesRead, err)
					}
					defer rr.Release()

					for rr.Next() && ctx.Err() == nil {
						rec := rr.RecordBatch()
						rec, err = recTransform(ctx, rec)
						if err != nil {
							return err
						}
						select {
						case chs[batchIdx] <- rec:
						case <-ctx.Done():
							rec.Release()
							return ctx.Err()
						}
					}
					return rr.Err()
				}
			}(batch, batchIdx))
		}

		// place this here so that we always clean up, but they can't be in a
		// separate goroutine. Otherwise we'll have a race condition between
		// the call to wait and the calls to group.Go to kick off the jobs
		// to perform the pre-fetching (GH-1283).
		rdr.err = group.Wait()
		// don't close the last channel until after the group is finished,
		// so that Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
	}()

	return rdr, nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.RecordBatch {
	return r.rec
}

func (r *reader) RecordBatch() arrow.RecordBatch {
	return r.rec
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}

	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()
		for _, ch := range r.chs {
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

var _ array.RecordReader = (*reader)(nil)
