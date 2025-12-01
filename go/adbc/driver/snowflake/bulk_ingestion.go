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
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const (
	bindStageName            = "ADBC$BIND"
	createTemporaryStageTmpl = "CREATE OR REPLACE TEMPORARY STAGE " + bindStageName + " FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE %s REPLACE_INVALID_CHARACTERS = TRUE)"
	vectorizedScannerOption  = "USE_VECTORIZED_SCANNER=TRUE"
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

	defaultVectorizedScanner bool = true

	ErrNoRecordsInStream = errors.New("no records in stream to write")
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
	// available network bandwidth, but will increase memory utilization.
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
	// Whether to use the vectorized scanner when ingesting Parquet files into Snowvake.
	//
	// Default is true.
	vectorizedScanner bool
}

func DefaultIngestOptions() *ingestOptions {
	return &ingestOptions{
		targetFileSize:    defaultTargetFileSize,
		writerConcurrency: defaultWriterConcurrency,
		uploadConcurrency: defaultUploadConcurrency,
		copyConcurrency:   defaultCopyConcurrency,
		compressionCodec:  defaultCompressionCodec,
		compressionLevel:  defaultCompressionLevel,
		vectorizedScanner: defaultVectorizedScanner,
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
	initialRows, err = countRowsInTable(ctx, st.cnxn.cn, target)
	if err != nil {
		st.bound.Release()
		return
	}

	parquetProps, arrowProps := newWriterProps(st.alloc, st.ingestOptions)
	g := errgroup.Group{}

	// writeParquet takes a channel of Records, but we only have one Record to write
	recordCh := make(chan arrow.RecordBatch, 1)
	recordCh <- st.bound
	close(recordCh)

	// Read the Record from the channel and write it into the provided writer
	schema := st.bound.Schema()
	r, w := io.Pipe()
	bw := bufio.NewWriter(w)
	g.Go(func() (err error) {
		defer func() {
			err = errors.Join(err, bw.Flush(), w.Close())
		}()

		err = writeParquet(schema, bw, recordCh, 0, parquetProps, arrowProps)
		if err != io.EOF {
			return err
		}
		return nil
	})

	// Create a temporary stage, we can't start uploading until it has been created
	var createTemporaryStageStmt string
	if st.ingestOptions.vectorizedScanner {
		createTemporaryStageStmt = fmt.Sprintf(createTemporaryStageTmpl, vectorizedScannerOption)
	} else {
		createTemporaryStageStmt = fmt.Sprintf(createTemporaryStageTmpl, "")
	}
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
	nrows, err = countRowsInTable(ctx, st.cnxn.cn, target)
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
	initialRows, err = countRowsInTable(ctx, st.cnxn.cn, target)
	if err != nil {
		return
	}

	defer func() {
		// Always check the resulting row count, even in the case of an error. We may have ingested part of the data.
		ctx := context.WithoutCancel(ctx)
		n, countErr := countRowsInTable(ctx, st.cnxn.cn, target)
		nrows = n - initialRows

		// Ingestion, row-count check, or both could have failed
		// Wrap any failures as ADBC errors
		err = errors.Join(err, countErr)
		if errors.Is(err, context.Canceled) {
			err = errToAdbcErr(adbc.StatusCancelled, err)
		} else {
			err = errToAdbcErr(adbc.StatusInternal, err)
		}
	}()

	parquetProps, arrowProps := newWriterProps(st.alloc, st.ingestOptions)
	g, gCtx := errgroup.WithContext(ctx)

	// Read records into channel
	records := make(chan arrow.RecordBatch, st.ingestOptions.writerConcurrency)
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
			pool,
			records,
			buffers,
		)
	})

	// Create a temporary stage, we can't start uploading until it has been created
	var createTemporaryStageStmt string
	if st.ingestOptions.vectorizedScanner {
		createTemporaryStageStmt = fmt.Sprintf(createTemporaryStageTmpl, vectorizedScannerOption)
	} else {
		createTemporaryStageStmt = fmt.Sprintf(createTemporaryStageTmpl, "")
	}
	_, err = st.cnxn.cn.ExecContext(ctx, createTemporaryStageStmt, nil)
	if err != nil {
		return
	}

	// Kickoff background tasks to COPY Parquet files into Snowflake table as they are uploaded
	fileReady, finishCopy, cancelCopy := runCopyTasks(ctx, st.cnxn.cn, target, int(st.ingestOptions.copyConcurrency))

	// Read Parquet files from buffer pool and upload to Snowflake stage in parallel
	g.Go(func() error {
		return uploadAllStreams(gCtx, st.cnxn.cn, buffers, int(st.ingestOptions.uploadConcurrency), pool, fileReady)
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

func readRecords(ctx context.Context, rdr array.RecordReader, out chan<- arrow.RecordBatch) error {
	defer close(out)

	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return rdr.Err()
}

func writeRecordToParquet(wr *pqarrow.FileWriter, rec arrow.RecordBatch) (int64, error) {
	if rec.NumRows() == 0 {
		rec.Release()
		return 0, nil
	}

	err := wr.Write(rec)
	rec.Release()
	if err != nil {
		return 0, err
	}

	return wr.RowGroupTotalBytesWritten(), nil
}

func writeParquet(
	schema *arrow.Schema,
	w io.Writer,
	in <-chan arrow.RecordBatch,
	targetSize int,
	parquetProps *parquet.WriterProperties,
	arrowProps pqarrow.ArrowWriterProperties,
) (err error) {
	pqWriter, err := pqarrow.NewFileWriter(schema, w, parquetProps, arrowProps)
	if err != nil {
		return err
	}
	defer func() {
		writerErr := pqWriter.Close()
		if writerErr != nil {
			if err == io.EOF {
				err = writerErr
			} else {
				err = errors.Join(err, writerErr)
			}
		}
	}()

	var bytesWritten int64
	for rec := range in {
		nbytes, err := writeRecordToParquet(pqWriter, rec)
		if err != nil {
			return err
		}

		bytesWritten += nbytes
		if targetSize < 0 {
			continue
		}
		if bytesWritten >= int64(targetSize) {
			return nil
		}
	}

	// let the caller know if the parquet file is empty, to avoid sending it any further in the pipeline
	if bytesWritten == 0 {
		return ErrNoRecordsInStream
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
	buffers *bufferPool,
	in <-chan arrow.RecordBatch,
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

			buf := buffers.GetBuffer()
			err := writeParquet(schema, buf, in, targetSize, parquetProps, arrowProps)
			if err == io.EOF {
				tryWriteBuffer(buf)
				finished()
				return nil
			}
			if errors.Is(err, ErrNoRecordsInStream) {
				// no records were written to the parquet file, so just return the buffer
				// to the pool instead of passing it along to the next pipeline stage
				buffers.PutBuffer(buf)
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
	buffers *bufferPool,
	uploadCallback func(string),
) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var i int
	// Loop through buffers as they become ready and assign to available upload workers
	for buf := range streams {
		select {
		case <-ctx.Done():
			// The context is canceled on error, so we wait for graceful shutdown of in-progress uploads.
			// The gosnowflake.snowflakeFileTransferAgent does not currently propagate context, so we
			// have to wait for uploads to finish for proper shutdown. (https://github.com/snowflakedb/gosnowflake/issues/1028)
			return g.Wait()
		default:
		}

		buf := buf // mutable loop variable
		fileName := fmt.Sprintf("%d.parquet", i)
		g.Go(func() error {
			defer buffers.PutBuffer(buf)
			defer uploadCallback(fileName)

			return uploadStream(ctx, cn, buf, fileName)
		})
		i++
	}
	return g.Wait()
}

func executeCopyQuery(ctx context.Context, cn snowflakeConn, tableName string, filesToCopy *fileSet) (err error) {
	rows, err := cn.QueryContext(ctx, copyQuery, []driver.NamedValue{{Value: tableName}})
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	fileColIdx := slices.Index(rows.Columns(), "file")
	if fileColIdx < 0 {
		// COPY response does not include 'file' column if no files we copied
		return nil
	}

	for {
		vals := make([]driver.Value, len(rows.Columns()))
		err := rows.Next(vals)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		file, ok := vals[fileColIdx].(string)
		if !ok {
			return fmt.Errorf("unexpected response for COPY, expected 'file' to be string, found: %T", vals[fileColIdx])
		}
		filesToCopy.Remove(file)
	}

	return nil
}

func runCopyTasks(ctx context.Context, cn snowflakeConn, tableName string, concurrency int) (func(string), func() error, func()) {
	var filesToCopy fileSet

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	if concurrency == 0 {
		concurrency = -1 // if concurrency is 0, then treat it as unlimited
	}
	g.SetLimit(concurrency)

	done := make(chan struct{})
	readyCh := make(chan struct{}, 1)
	stopCh := make(chan interface{})

	// Handler to signal that new data has been uploaded.
	// Executing COPY will be a no-op if this has not been called since the last COPY was dispatched, so we wait for the signal.
	readyFn := func(filename string) {
		// keep track of each file uploaded to the stage, until it has been copied into the table successfully
		filesToCopy.Add(filename)

		// readyFn is a no-op if the shutdown signal has already been received
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

		<-done

		// wait for any currently running copies to finish before we continue
		if err := g.Wait(); err != nil {
			return err
		}

		if filesToCopy.Len() == 0 {
			return nil
		}

		maxRetries := 5 // maybe make configurable?
		for attempt := 0; attempt < maxRetries+1; attempt++ {
			if attempt > 0 {
				// backoff by 100ms, 200ms, 400ms, ...
				factor := time.Duration(math.Pow(2, float64(attempt-1)))
				backoff := 100 * factor * time.Millisecond
				time.Sleep(backoff)
			}

			if err := executeCopyQuery(ctx, cn, tableName, &filesToCopy); err != nil {
				return err
			}

			if filesToCopy.Len() == 0 {
				// all files successfully copied
				return nil
			}
		}

		return fmt.Errorf("some files not loaded by COPY command, %d files remain after %d retries", filesToCopy.Len(), maxRetries)
	}

	// Handler to signal that ingestion pipeline failed and COPY operations should not proceed.
	// Stops the dispatch of new bg workers and cancels all in-progress COPY operations.
	cancelFn := func() {
		defer cancel()
		close(stopCh)
		close(readyCh)
	}

	go func() {
		defer close(done)
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
				return executeCopyQuery(ctx, cn, tableName, &filesToCopy)
			})
		}
	}()

	return readyFn, stopFn, cancelFn
}

func countRowsInTable(ctx context.Context, db snowflakeConn, tableName string) (rowCount int64, err error) {
	rows, err := db.QueryContext(ctx, countQuery, []driver.NamedValue{{Value: tableName}})
	if err != nil {
		return 0, errToAdbcErr(adbc.StatusIO, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return 0, errToAdbcErr(adbc.StatusIO, err)
	}

	n, err := strconv.Atoi(dest[0].(string))
	return int64(n), err
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
	return bp.Get().(*bytes.Buffer)
}

func (bp *bufferPool) PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bp.Put(buf)
}

type fileSet sync.Map

func (s *fileSet) Add(file string) {
	basename := path.Base(file)
	(*sync.Map)(s).Store(basename, nil)
}

func (s *fileSet) Remove(file string) {
	basename := path.Base(file)
	(*sync.Map)(s).Delete(basename)

}

func (s *fileSet) Len() int {
	var items int
	(*sync.Map)(s).Range(func(key, value any) bool {
		items++
		return true
	})
	return items
}
