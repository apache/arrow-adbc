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
	"context"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

func identCol(_ context.Context, a arrow.Array) (arrow.Array, error) {
	a.Retain()
	return a, nil
}

type recordTransformer = func(context.Context, arrow.Record) (arrow.Record, error)
type colTransformer = func(context.Context, arrow.Array) (arrow.Array, error)

func getRecTransformer(sc *arrow.Schema, tr []colTransformer) recordTransformer {
	return func(ctx context.Context, r arrow.Record) (arrow.Record, error) {
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

		return array.NewRecord(sc, cols, r.NumRows()), nil
	}
}

func getTransformer(sc *arrow.Schema, ld gosnowflake.ArrowStreamLoader, useHighPrecision bool) (*arrow.Schema, recordTransformer) {
	loc, types := ld.Location(), ld.RowTypes()

	fields := make([]arrow.Field, len(sc.Fields()))
	transformers := make([]func(context.Context, arrow.Array) (arrow.Array, error), len(sc.Fields()))
	for i, f := range sc.Fields() {
		srcMeta := types[i]

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
			dt := &arrow.TimestampType{Unit: arrow.TimeUnit(srcMeta.Scale / 3)}
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

					v, err := arrow.TimestampFromTime(time.Unix(epoch[i], int64(fraction[i])), dt.TimeUnit())
					if err != nil {
						return nil, err
					}
					tb.Append(v)
				}
				return tb.NewArray(), nil
			}
		case "TIMESTAMP_LTZ":
			dt := &arrow.TimestampType{Unit: arrow.TimeUnit(srcMeta.Scale) / 3, TimeZone: loc.String()}
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

						v, err := arrow.TimestampFromTime(time.Unix(epoch[i], int64(fraction[i])), dt.TimeUnit())
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

						q := int64(t) / int64(math.Pow10(int(srcMeta.Scale)))
						r := int64(t) % int64(math.Pow10(int(srcMeta.Scale)))
						v, err := arrow.TimestampFromTime(time.Unix(q, r), dt.Unit)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				}
				return tb.NewArray(), nil
			}
		case "TIMESTAMP_TZ":
			// we convert each value to UTC since we have timezone information
			// with the data that lets us do so.
			dt := &arrow.TimestampType{TimeZone: "UTC", Unit: arrow.TimeUnit(srcMeta.Scale / 3)}
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
						v, err := arrow.TimestampFromTime(time.Unix(epoch[i], 0).In(loc), dt.Unit)
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

						loc := gosnowflake.Location(int(tzoffset[i]) - 1440)
						v, err := arrow.TimestampFromTime(time.Unix(epoch[i], int64(fraction[i])).In(loc), dt.Unit)
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

	meta := sc.Metadata()
	out := arrow.NewSchema(fields, &meta)
	return out, getRecTransformer(out, transformers)
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

func rowTypesToArrowSchema(ctx context.Context, ld gosnowflake.ArrowStreamLoader, useHighPrecision bool) (*arrow.Schema, error) {
	var loc *time.Location

	metadata := ld.RowTypes()
	fields := make([]arrow.Field, len(metadata))
	for i, srcMeta := range metadata {
		fields[i] = arrow.Field{
			Name:     srcMeta.Name,
			Nullable: srcMeta.Nullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				"SNOWFLAKE_TYPE": srcMeta.Type,
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
				fields[i].Type = arrow.PrimitiveTypes.Int64
			}
		case "real":
			fields[i].Type = arrow.PrimitiveTypes.Float64
		case "date":
			fields[i].Type = arrow.PrimitiveTypes.Date32
		case "time":
			fields[i].Type = arrow.FixedWidthTypes.Time64ns
		case "timestamp_ntz", "timestamp_tz":
			fields[i].Type = arrow.FixedWidthTypes.Timestamp_ns
		case "timestamp_ltz":
			if loc == nil {
				loc = ld.Location()
			}
			fields[i].Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
		case "binary":
			fields[i].Type = arrow.BinaryTypes.Binary
		default:
			fields[i].Type = arrow.BinaryTypes.String
		}
	}
	return arrow.NewSchema(fields, nil), nil
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

func jsonDataToArrow(ctx context.Context, bldr *array.RecordBuilder, ld gosnowflake.ArrowStreamLoader) (arrow.Record, error) {
	rawData := ld.JSONData()
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
				tz, err := fb.Type().(*arrow.TimestampType).GetZone()
				if err != nil {
					return nil, err
				}

				if tz != time.UTC {
					sec, nsec, err := extractTimestamp(col)
					if err != nil {
						return nil, err
					}

					val := time.Unix(sec, nsec).In(tz)
					ts, err := arrow.TimestampFromTime(val, arrow.Nanosecond)
					if err != nil {
						return nil, err
					}
					fb.Append(ts)
					break
				}

				snowflakeType, _ := bldr.Schema().Field(i).Metadata.GetValue("SNOWFLAKE_TYPE")
				if snowflakeType == "timestamp_ntz" {
					sec, nsec, err := extractTimestamp(col)
					if err != nil {
						return nil, err
					}

					fb.Append(arrow.Timestamp(sec*1e9 + nsec))
					break
				}

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
				ts, err := arrow.TimestampFromTime(tt, arrow.Nanosecond)
				if err != nil {
					return nil, err
				}
				fb.Append(ts)
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
	return bldr.NewRecord(), nil
}

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.Record
	curChIndex int
	rec        arrow.Record
	err        error

	cancelFn context.CancelFunc
}

func newRecordReader(ctx context.Context, alloc memory.Allocator, ld gosnowflake.ArrowStreamLoader, bufferSize, prefetchConcurrency int, useHighPrecision bool) (array.RecordReader, error) {
	batches, err := ld.GetBatches()
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err)
	}

	if len(batches) == 0 {
		schema, err := rowTypesToArrowSchema(ctx, ld, useHighPrecision)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInternal,
			}
		}

		bldr := array.NewRecordBuilder(alloc, schema)
		defer bldr.Release()

		rec, err := jsonDataToArrow(ctx, bldr, ld)
		if err != nil {
			return nil, err
		}
		defer rec.Release()

		if ld.TotalRows() != 0 {
			return array.NewRecordReader(schema, []arrow.Record{rec})
		} else {
			return array.NewRecordReader(schema, []arrow.Record{})
		}
	}

	ch := make(chan arrow.Record, bufferSize)
	r, err := batches[0].GetStream(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	rr, err := ipc.NewReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidState,
		}
	}

	group, ctx := errgroup.WithContext(compute.WithAllocator(ctx, alloc))
	ctx, cancelFn := context.WithCancel(ctx)

	schema, recTransform := getTransformer(rr.Schema(), ld, useHighPrecision)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	group.SetLimit(prefetchConcurrency)
	group.Go(func() error {
		defer rr.Release()
		defer r.Close()
		if len(batches) > 1 {
			defer close(ch)
		}

		for rr.Next() && ctx.Err() == nil {
			rec := rr.Record()
			rec, err = recTransform(ctx, rec)
			if err != nil {
				return err
			}
			ch <- rec
		}
		return rr.Err()
	})

	chs := make([]chan arrow.Record, len(batches))
	chs[0] = ch
	rdr := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   schema,
	}

	lastChannelIndex := len(chs) - 1
	go func() {
		for i, b := range batches[1:] {
			batch, batchIdx := b, i+1
			chs[batchIdx] = make(chan arrow.Record, bufferSize)
			group.Go(func() error {
				// close channels (except the last) so that Next can move on to the next channel properly
				if batchIdx != lastChannelIndex {
					defer close(chs[batchIdx])
				}

				rdr, err := batch.GetStream(ctx)
				if err != nil {
					return err
				}
				defer rdr.Close()

				rr, err := ipc.NewReader(rdr, ipc.WithAllocator(alloc))
				if err != nil {
					return err
				}
				defer rr.Release()

				for rr.Next() && ctx.Err() == nil {
					rec := rr.Record()
					rec, err = recTransform(ctx, rec)
					if err != nil {
						return err
					}
					chs[batchIdx] <- rec
				}

				return rr.Err()
			})
		}
	}()

	go func() {
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

func (r *reader) Record() arrow.Record {
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
