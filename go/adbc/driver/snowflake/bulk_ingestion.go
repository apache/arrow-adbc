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
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const (
	bindStageName            = "ADBC$BIND"
	createTemporaryStageStmt = "CREATE OR REPLACE TEMPORARY STAGE " + bindStageName + " FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE)"
	putQueryTmpl             = "PUT 'file:///tmp/placeholder/%s' @" + bindStageName + " OVERWRITE = TRUE"
	copyQuery                = "COPY INTO IDENTIFIER(?) FROM @" + bindStageName + " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
	countQuery               = "SELECT COUNT(*) FROM IDENTIFIER(?)"
	megabyte                 = 1024 * 1024
)

var (
	defaultTargetFileSize    uint = 10 * megabyte
	defaultWriterConcurrency uint = uint(runtime.NumCPU())
	defaultUploadConcurrency uint = 8
	defaultCopyConcurrency   uint = 4

	defaultCompressionCodec compress.Compression = compress.Codecs.Snappy
	defaultCompressionLevel int                  = flate.DefaultCompression
)

// Options for configuring bulk ingestion.
//
// Values should be updated with appropriate calls to stmt.SetOption().
type ingestOptions struct {
	// Approximate size of Parquet files written during ingestion.
	//
	// Actual size will be slightly larger, depending on size of footer/metadata.
	// Default is 10 MB. If set to 0, file size has no limit. Cannot be negative.
	targetFileSize uint
	// Number of Parquet files to write in parallel.
	//
	// Default attempts to maximize workers based on logical cores detected, but
	// may need to be adjusted if running in a constrained environment.
	// If set to 0, default value is used. Cannot be negative.
	writerConcurrency uint
	// Number of Parquet files to upload in parallel.
	//
	// Greater concurrency can smooth out TCP congestion and help make use of
	// available network bandwith, but will increase memory utilization.
	// Default is 8. If set to 0, default value is used. Cannot be negative.
	uploadConcurrency uint
	// Maximum number of COPY operations to run concurrently.
	//
	// Bulk ingestion performance is optimized by executing COPY queries as files are
	// still being uploaded. Snowflake COPY speed scales with warehouse size, so smaller
	// warehouses may benefit from setting this value higher to ensure long-running
	// COPY queries do not block newly uploaded files from being loaded.
	// Default is 4. If set to 0, only a single COPY query will be executed as part of ingestion,
	// once all files have finished uploading. Cannot be negative.
	copyConcurrency uint
	// Compression codec to use for Parquet files.
	//
	// When network speeds are high, it is generally faster to use a faster codec with
	// a lower compression ratio. The opposite is true if the network is slow by CPU is
	// available.
	// Default is Snappy.
	compressionCodec compress.Compression
	// Compression level for Parquet files.
	//
	// The compression level is codec-specific. Some codecs do not support setting it,
	// notably Snappy.
	// Default is the default level for the specified compressionCodec.
	compressionLevel int
}

func DefaultIngestOptions() *ingestOptions {
	return &ingestOptions{
		targetFileSize:    defaultTargetFileSize,
		writerConcurrency: defaultWriterConcurrency,
		uploadConcurrency: defaultUploadConcurrency,
		copyConcurrency:   defaultCopyConcurrency,
		compressionCodec:  defaultCompressionCodec,
		compressionLevel:  defaultCompressionLevel,
	}
}

// ingestRecord performs bulk ingestion of a single Record and returns the
// number of rows affected.
//
// The Record must already be bound by calling stmt.Bind(), and will be released
// and reset upon completion.
func (st *statement) ingestRecord(ctx context.Context) (nrows int64, err error) {
	defer func() {
		// Record already released by writeParquet()
		st.bound = nil
	}()

	var (
		initialRows int64
		target      = quoteTblName(st.targetTable)
	)

	// Check final row count of target table to get definitive rows affected
	initialRows, err = countRowsInTable(ctx, st.cnxn.sqldb, target)
	if err != nil {
		st.bound.Release()
		return
	}

	parquetProps, arrowProps := newWriterProps(st.alloc, st.ingestOptions)
	g := errgroup.Group{}

	// writeParquet takes a channel of Records, but we only have one Record to write
	recordCh := make(chan arrow.Record, 1)
	recordCh <- st.bound
	close(recordCh)

	// Read the Record from the channel and write it into the provided writer
	schema := st.bound.Schema()
	r, w := io.Pipe()
	bw := bufio.NewWriter(w)
	g.Go(func() error {
		defer r.Close()
		defer bw.Flush()

		err = writeParquet(schema, bw, recordCh, 0, parquetProps, arrowProps)
		if err != io.EOF {
			return err
		}
		return nil
	})

	// Create a temporary stage, we can't start uploading until it has been created
	_, err = st.cnxn.cn.ExecContext(ctx, createTemporaryStageStmt, nil)
	if err != nil {
		return
	}

	// Start uploading the file to Snowflake
	fileName := "0.parquet" // Only writing 1 file, so use same name as first file written by ingestStream() for consistency
	err = uploadStream(ctx, st.cnxn.cn, r, fileName)
	if err != nil {
		return
	}

	// Parquet writing is already done if the upload finished, so we're just checking for any errors
	err = g.Wait()
	if err != nil {
		return
	}

	// Load the uploaded file into the target table
	_, err = st.cnxn.cn.ExecContext(ctx, copyQuery, []driver.NamedValue{{Value: target}})
	if err != nil {
		return
	}

	// Check final row count of target table to get definitive rows affected
	nrows, err = countRowsInTable(ctx, st.cnxn.sqldb, target)
	nrows = nrows - initialRows
	return
}

// ingestStream performs bulk ingestion of a RecordReader and returns the
// number of rows affected.
//
// The RecordReader must already be bound by calling stmt.BindStream(), and will
// be released and reset upon completion.
func (st *statement) ingestStream(ctx context.Context) (nrows int64, err error) {
	defer func() {
		st.streamBind.Release()
		st.streamBind = nil
	}()

	var (
		initialRows int64
		target      = quoteTblName(st.targetTable)
	)

	// Check final row count of target table to get definitive rows affected
	initialRows, err = countRowsInTable(ctx, st.cnxn.sqldb, target)
	if err != nil {
		return
	}

	defer func() {
		// Always check the resulting row count, even in the case of an error. We may have ingested part of the data.
		ctx := context.Background() // TODO(joellubi): switch to context.WithoutCancel(ctx) once we're on Go 1.21
		n, countErr := countRowsInTable(ctx, st.cnxn.sqldb, target)
		nrows = n - initialRows

		// Ingestion, row-count check, or both could have failed
		// Wrap any failures as ADBC errors

		// TODO(joellubi): simplify / improve with errors.Join(err, countErr) once we're on Go 1.20
		if err == nil {
			err = errToAdbcErr(adbc.StatusInternal, countErr)
			return
		}

		// Failure in the pipeline itself
		if errors.Is(err, context.Canceled) {
			err = errToAdbcErr(adbc.StatusCancelled, err)
		} else {
			err = errToAdbcErr(adbc.StatusInternal, err)
		}
	}()

	parquetProps, arrowProps := newWriterProps(st.alloc, st.ingestOptions)
	g, gCtx := errgroup.WithContext(ctx)

	// Read records into channel
	records := make(chan arrow.Record, st.ingestOptions.writerConcurrency)
	g.Go(func() error {
		return readRecords(gCtx, st.streamBind, records)
	})

	// Read records from channel and write Parquet files in parallel to buffer pool
	schema := st.streamBind.Schema()
	pool := newBufferPool(int(st.ingestOptions.targetFileSize))
	buffers := make(chan *bytes.Buffer, st.ingestOptions.writerConcurrency)
	g.Go(func() error {
		return runParallelParquetWriters(
			gCtx,
			schema,
			int(st.ingestOptions.targetFileSize),
			int(st.ingestOptions.writerConcurrency),
			parquetProps,
			arrowProps,
			pool.GetBuffer,
			records,
			buffers,
		)
	})

	// Create a temporary stage, we can't start uploading until it has been created
	_, err = st.cnxn.cn.ExecContext(ctx, createTemporaryStageStmt, nil)
	if err != nil {
		return
	}

	// Kickoff background tasks to COPY Parquet files into Snowflake table as they are uploaded
	fileReady, finishCopy, cancelCopy := runCopyTasks(ctx, st.cnxn.cn, target, int(st.ingestOptions.copyConcurrency))

	// Read Parquet files from buffer pool and upload to Snowflake stage in parallel
	g.Go(func() error {
		return uploadAllStreams(gCtx, st.cnxn.cn, buffers, int(st.ingestOptions.uploadConcurrency), pool.PutBuffer, fileReady)
	})

	// Wait until either all files have been uploaded to Snowflake or the pipeline has failed / been canceled
	if err = g.Wait(); err != nil {
		// If the pipeline failed, we can cancel in-progress COPYs and avoid starting the final one
		cancelCopy()
		return
	}

	// Start final COPY and wait for all COPY tasks to complete
	err = finishCopy()
	return
}

func newWriterProps(mem memory.Allocator, opts *ingestOptions) (*parquet.WriterProperties, pqarrow.ArrowWriterProperties) {
	parquetProps := parquet.NewWriterProperties(
		parquet.WithAllocator(mem),
		parquet.WithCompression(opts.compressionCodec),
		parquet.WithCompressionLevel(opts.compressionLevel),
		// Overhead for dict building often reduces throughput more than filesize reduction benefits; may expose as config in future
		parquet.WithDictionaryDefault(false),
		// Stats won't be used since the file is dropped after ingestion completes
		parquet.WithStats(false),
		parquet.WithMaxRowGroupLength(math.MaxInt64),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))

	return parquetProps, arrowProps
}

func readRecords(ctx context.Context, rdr array.RecordReader, out chan<- arrow.Record) error {
	defer close(out)

	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func writeParquet(
	schema *arrow.Schema,
	w io.Writer,
	in <-chan arrow.Record,
	targetSize int,
	parquetProps *parquet.WriterProperties,
	arrowProps pqarrow.ArrowWriterProperties,
) error {
	limitWr := &limitWriter{w: w, limit: targetSize}
	pqWriter, err := pqarrow.NewFileWriter(schema, limitWr, parquetProps, arrowProps)
	if err != nil {
		return err
	}
	defer pqWriter.Close()

	for rec := range in {
		err = pqWriter.Write(rec)
		rec.Release()
		if err != nil {
			return err
		}
		if limitWr.LimitExceeded() {
			return nil
		}
	}

	// Input channel closed, signal that all parquet writing is done
	return io.EOF
}

func runParallelParquetWriters(
	ctx context.Context,
	schema *arrow.Schema,
	targetSize int,
	concurrency int,
	parquetProps *parquet.WriterProperties,
	arrowProps pqarrow.ArrowWriterProperties,
	newBuffer func() *bytes.Buffer,
	in <-chan arrow.Record,
	out chan<- *bytes.Buffer,
) error {
	var once sync.Once
	defer close(out)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	done := make(chan interface{})
	finished := func() {
		once.Do(func() { close(done) })
	}

	tryWriteBuffer := func(buf *bytes.Buffer) {
		select {
		case out <- buf:
		case <-ctx.Done():
			// If the context is canceled, the buffer may be full but we don't want to block indefinitely
		}
	}

	for {
		select {
		case <-done:
			return g.Wait()
		default:
		}

		g.Go(func() error {
			select {
			case <-done:
				// Channel may have already closed while goroutine was waiting to get scheduled
				return nil
			case <-ctx.Done():
				finished()
				return ctx.Err()
			default:
				// Proceed to write a new file
			}

			buf := newBuffer()
			err := writeParquet(schema, buf, in, targetSize, parquetProps, arrowProps)
			if err == io.EOF {
				tryWriteBuffer(buf)
				finished()
				return nil
			}
			if err == nil {
				tryWriteBuffer(buf)
			}
			return err
		})
	}
}

func uploadStream(ctx context.Context, cn snowflakeConn, r io.Reader, name string) error {
	putQuery := fmt.Sprintf(putQueryTmpl, name)
	putQuery = strings.ReplaceAll(putQuery, "\\", "\\\\") // Windows compatibility

	_, err := cn.ExecContext(gosnowflake.WithFileStream(ctx, r), putQuery, nil)
	if err != nil {
		return err
	}

	return nil
}

func uploadAllStreams(
	ctx context.Context,
	cn snowflakeConn,
	streams <-chan *bytes.Buffer,
	concurrency int,
	freeBuffer func(*bytes.Buffer),
	uploadCallback func(),
) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var i int
	// Loop through buffers as they become ready and assign to available upload workers
	for buf := range streams {
		select {
		case <-ctx.Done():
			// The context is canceled on error, so we wait for graceful shutdown of in-progress uploads.
			// The gosnowflake.snowflakeFileTransferAgent does not currently propogate context, so we
			// have to wait for uploads to finish for proper shutdown. (https://github.com/snowflakedb/gosnowflake/issues/1028)
			return g.Wait()
		default:
		}

		buf := buf // mutable loop variable
		fileName := fmt.Sprintf("%d.parquet", i)
		g.Go(func() error {
			defer freeBuffer(buf)
			defer uploadCallback()

			return uploadStream(ctx, cn, buf, fileName)
		})
		i++
	}
	return g.Wait()
}

func runCopyTasks(ctx context.Context, cn snowflakeConn, tableName string, concurrency int) (func(), func() error, func()) {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	readyCh := make(chan struct{}, 1)
	stopCh := make(chan interface{})

	// Handler to signal that new data has been uploaded.
	// Executing COPY will be a no-op if this has not been called since the last COPY was dispatched, so we wait for the signal.
	readyFn := func() {

		// readyFn is a no-op if the shutdown signal has already been recieved
		select {
		case _, ok := <-stopCh:
			if !ok {
				return
			}
		default:
			// Continue
		}

		// readyFn is a no-op if it already knows that at least 1 file is ready to be loaded
		select {
		case readyCh <- struct{}{}:
		default:
			return
		}
	}

	// Handler to signal that ingestion pipeline has completed successfully.
	// Executes COPY to finalize ingestion (may no-op if all files already loaded by bg workers) and waits for all COPYs to complete.
	stopFn := func() error {
		defer cancel()
		close(stopCh)
		close(readyCh)

		_, err := cn.ExecContext(ctx, copyQuery, []driver.NamedValue{{Value: tableName}})
		if err != nil {
			return err
		}

		return g.Wait()
	}

	// Handler to signal that ingestion pipeline failed and COPY operations should not proceed.
	// Stops the dispatch of new bg workers and cancels all in-progress COPY operations.
	cancelFn := func() {
		defer cancel()
		close(stopCh)
		close(readyCh)
	}

	go func() {
		for {

			// Block until there is at least 1 new file available for copy, or it's time to shutdown
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			case _, ok := <-readyCh:
				if !ok {
					return
				}
				// Proceed to start a new COPY job
			}

			g.Go(func() error {
				_, err := cn.ExecContext(ctx, copyQuery, []driver.NamedValue{{Value: tableName}})
				return err
			})
		}
	}()

	return readyFn, stopFn, cancelFn
}

func countRowsInTable(ctx context.Context, db *sql.DB, tableName string) (int64, error) {
	var nrows int64

	row := db.QueryRowContext(ctx, countQuery, tableName)
	if err := row.Scan(&nrows); err != nil {
		return 0, errToAdbcErr(adbc.StatusIO, err)
	}

	return nrows, nil
}

// Initializes a sync.Pool of *bytes.Buffer.
// Extra space is preallocated so that the Parquet footer can be written after reaching target file size without growing the buffer
func newBufferPool(size int) *bufferPool {
	buffers := sync.Pool{
		New: func() interface{} {
			extraSpace := 1 * megabyte // TODO(joellubi): Generally works, but can this be smarter?
			buf := make([]byte, 0, size+extraSpace)
			return bytes.NewBuffer(buf)
		},
	}

	return &bufferPool{&buffers}
}

type bufferPool struct {
	*sync.Pool
}

func (bp *bufferPool) GetBuffer() *bytes.Buffer {
	return bp.Pool.Get().(*bytes.Buffer)
}

func (bp *bufferPool) PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bp.Pool.Put(buf)
}

// Wraps an io.Writer and specifies a limit.
// Keeps track of how many bytes have been written and can report whether the limit has been exceeded.
// TODO(ARROW-39789): We prefer to use RowGroupTotalBytesWritten on the ParquetWriter, but there seems to be a discrepency with the count.
type limitWriter struct {
	w     io.Writer
	limit int

	bytesWritten int
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	n, err := lw.w.Write(p)
	lw.bytesWritten += n

	return n, err
}

func (lw *limitWriter) LimitExceeded() bool {
	if lw.limit > 0 {
		return lw.bytesWritten > lw.limit
	}
	// Limit disabled
	return false
}
