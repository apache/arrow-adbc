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
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

type chunkResponse struct {
	chunkIndex int
	inner      *http.Response
	err        error
}

type reader struct {
	refCount int64

	stmtExecution sql.StatementExecutionInterface
	httpClient    *http.Client

	// Statement that this reader is associated with.
	StatementId string

	// Fields from the execution response manifest:

	// Array of result set chunk metadata.
	Chunks []sql.BaseChunkInfo
	// The total number of chunks that the result set has been divided into.
	TotalChunkCount int
	// The total number of rows in the result set.
	TotalRowCount int64

	// The chunk index that is currently being loaded in the background or -1.
	loadingChunkIdx int
	// The channel to receive chunk responses.
	chunkChan chan chunkResponse
	// The underlying reader for the activeChunk.
	activeChunkBody io.ReadCloser
	// The reader for the already loaded chunk. If nil, poll for the next chunk.
	activeChunk array.RecordReader

	schema *arrow.Schema
	rec    arrow.Record
	err    error

	cancelFn context.CancelFunc

	// Statistics

	// Reader's start time.
	startTime time.Time
	// All the bytes received from the server.
	BytesReceived int64
	// Time spent waiting for the server to respond in the foreground.
	WaitTime time.Duration
}

func NewRecordReader(
	stmtExecution sql.StatementExecutionInterface, statementId string, result *sql.ResultData, manifest *sql.ResultManifest) (*reader, error) {
	r := &reader{
		refCount: 1,

		// TODO: context
		stmtExecution: stmtExecution,
		httpClient:    http.DefaultClient,

		StatementId: statementId,

		Chunks:          manifest.Chunks,
		TotalChunkCount: manifest.TotalChunkCount,
		TotalRowCount:   manifest.TotalRowCount,

		loadingChunkIdx: 0,
		chunkChan:       make(chan chunkResponse),
		activeChunk:     nil,

		schema: nil, // TODO: build schema when there are no chunks
		rec:    nil,
		err:    nil,

		cancelFn: func() {},

		startTime: time.Now(),
		// All the bytes received from the server.
		BytesReceived: 0,
		// Time spent waiting for the server to respond in the foreground.
		WaitTime: 0,
	}
	if len(manifest.Chunks) > 0 && len(result.ExternalLinks) > 0 {
		// Establish INVARIANT I by starting the loading of r.loadingChunkIdx
		go r.startChunkDataRequest(r.loadingChunkIdx, &result.ExternalLinks[0])
	} else {
		// Establish INVARIANT I by finishing the entire iteration process
		close(r.chunkChan)
		r.loadingChunkIdx = -1
	}
	return r, nil
}
func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.activeChunk != nil {
			r.activeChunk.Release()
			r.activeChunkBody.Close()
		}
		if r.rec != nil {
			r.rec.Release()
		}
		// TODO: cancel HTTP connection
		// TODO: close channel
		r.cancelFn()
	}
}

// \pre: loadingChunkIdx != -1 implies chunk is loading in the background (INVARIANT I)
// \pre: loadingChunkIdx == -1 implies chunkChan is closed (INVARIANT II)
// \post: if returns true, r.Record() != nil && r.err == nil
// \post: if returns false, r.Record() == nil and r.err *MUST* be checked
func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if r.err != nil {
		return false // post-condition holds: r.rec == nil && r.err != nil
	}
	// PROPERTY I: r.rec == nil && r.err == nil

	// If we don't have an active chunk, we need to wait for the loading one,
	// parse it, and trigger a request for the next chunk to preserve
	// invariants I and II.
	if r.activeChunk == nil {
		if r.loadingChunkIdx == -1 {
			return false // post-condition holds because of PROPERTY I
		}
		// wait for the loading chunk as we need it now
		startWait := time.Now()
		chunk := <-r.chunkChan
		r.BytesReceived += chunk.inner.ContentLength
		r.WaitTime += time.Since(startWait)
		if chunk.err != nil {
			r.err = chunk.err
			close(r.chunkChan)
			return false // post-condition holds because of PROPERTY I
		}
		if chunk.chunkIndex != r.loadingChunkIdx {
			log.Fatalf("expected chunk %d, but received %d", r.loadingChunkIdx, chunk.chunkIndex)
		}
		// trigger a request for a new chunk in the background
		r.loadingChunkIdx += 1
		if r.loadingChunkIdx < len(r.Chunks) {
			// INVARIANT I and II are preserved: loadingChunkIdx is loading in the background
			go r.startChunkDataRequest(r.loadingChunkIdx, nil)
		} else {
			// INVARIANT I and II are preserved: loadingChunkIdx is -1, so chunkChan is closed
			r.loadingChunkIdx = -1
			close(r.chunkChan)
		}
		// parse the new chunk into a record reader
		r.activeChunkBody = chunk.inner.Body
		chunkReader, err := ipc.NewReader(r.activeChunkBody)
		if err != nil {
			r.err = err
			return false
		}
		r.activeChunk = array.RecordReader(chunkReader)
		r.activeChunk.Retain()
		// make sure r.schema is set when the first chunk is parsed
		if r.schema != nil {
			r.schema = r.activeChunk.Schema()
		}
	}
	// PROPERTY II: r.activeChunk != nil

	if r.activeChunk.Next() {
		r.rec = r.activeChunk.Record()
		r.rec.Retain()
		return true // post-condition holds: r.rec != nil
	}
	// make sure the error (if it exists) is retained
	r.err = r.activeChunk.Err()
	// release the fully consumed (or err'd) chunk and close the underlying HTTP body stream
	r.activeChunk.Release()
	r.activeChunk = nil
	r.activeChunkBody.Close()
	// PROPERTY III: r.activeChunk == nil

	// Recursively call Next() to start processing another chunk or stopping.
	// Iteration will either terminate (if r.err != nil) or an attempt will be made
	// to load the next chunk (because r.activeChunk == nil) guaranteeing progress.
	return r.Next()
}

func (r *reader) Schema() *arrow.Schema {
	if r.schema == nil {
		if r.activeChunk == nil {
			if r.loadingChunkIdx == -1 {
				return nil // TODO: need to derive schema from the JSON manifest :(
			}
			// wait for the loading chunk
			startWait := time.Now()
			chunk := <-r.chunkChan
			r.WaitTime += time.Since(startWait)
			if chunk.err != nil {
				r.err = chunk.err
				close(r.chunkChan)
				return nil
			}
			if chunk.chunkIndex != r.loadingChunkIdx {
				log.Fatalf("expected chunk %d, but receiving %d", r.loadingChunkIdx, chunk.chunkIndex)
			}
			// trigger a request for a new chunk in the background
			r.loadingChunkIdx += 1
			if r.loadingChunkIdx < len(r.Chunks) {
				// INVARIANT I and II are preserved
				go r.startChunkDataRequest(r.loadingChunkIdx, nil)
			} else {
				// INVARIANT I and II are preserved
				r.loadingChunkIdx = -1
				close(r.chunkChan)
			}
			// parse the new chunk into a record reader
			r.activeChunkBody = chunk.inner.Body
			chunkReader, err := ipc.NewReader(r.activeChunkBody)
			if err != nil {
				r.err = err
				return nil
			}
			r.activeChunk = array.RecordReader(chunkReader)
			r.activeChunk.Retain()
		}
		r.schema = r.activeChunk.Schema()
	}
	return r.schema
}

// \pre: Next() returned true
func (r *reader) Record() arrow.Record {
	return r.rec
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Throughput() float64 {
	elapsed := time.Since(r.startTime)
	elapsedSeconds := elapsed.Seconds()
	return float64(r.BytesReceived) / elapsedSeconds
}

// Start an HTTP request for the chunk data and notify the chunkReceived channel
// when a response is received and data is available for streaming.
//
// NOTE: The caller is responsible for closing the response body in .inner.
func (r *reader) startChunkDataRequest(chunkIndex int, externalLink *sql.ExternalLink) {
	url := ""
	if externalLink != nil {
		url = externalLink.ExternalLink
	} else {
		// TODO(felipecrv): retry logic
		req := sql.GetStatementResultChunkNRequest{
			ChunkIndex:  chunkIndex,
			StatementId: r.StatementId,
		}
		res, err := r.stmtExecution.GetStatementResultChunkN(context.TODO(), req)
		if err != nil {
			r.chunkChan <- chunkResponse{
				chunkIndex: chunkIndex,
				inner:      nil,
				err:        err,
			}
			return
		} else {
			externalLink = &res.ExternalLinks[0]
			url = externalLink.ExternalLink
		}
	}
	// TODO: must send request headers as well
	// TODO: use context for cancellation
	res, err := r.httpClient.Get(url)
	r.chunkChan <- chunkResponse{
		chunkIndex: chunkIndex,
		inner:      res,
		err:        err,
	}
}
