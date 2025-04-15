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

	stmtExecution *sql.StatementExecutionInterface
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
}

func NewRecordReader(
	stmtExecution *sql.StatementExecutionInterface, statementId string, result *sql.ResultData, manifest *sql.ResultManifest) (*reader, error) {
	r := &reader{
		refCount: 1,

		stmtExecution: stmtExecution,
		httpClient:    http.DefaultClient,

		StatementId: statementId,

		Chunks:          manifest.Chunks,
		TotalChunkCount: manifest.TotalChunkCount,
		TotalRowCount:   manifest.TotalRowCount,

		loadingChunkIdx: 0,
		chunkChan:       make(chan chunkResponse),
		activeChunk:     nil,

		schema: nil, // XXX
		rec:    nil,
		err:    nil,

		cancelFn: func() {},
	}
	if len(manifest.Chunks) > 0 && len(result.ExternalLinks) > 0 {
		// Establish INVARIANT I by starting the loading of r.loadingChunkIdx
		go r.startChunkDataRequest(r.loadingChunkIdx, &result.ExternalLinks[0])
	} else {
		log.Fatal("manifest.Chunks is inconsistent with result.ExternalLinks")
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
		r.cancelFn()
	}
}

// \pre: INVARIANT I: loadingChunkIdx != -1 implies chunk is loading in the background
// \pre: INVARIANT II: loadingChunkIdx == -1 implies chunkChan is closed
// \post: if returns true, r.Record() != nil && r.err == nil
// \post: if returns false, r.Record() == nil and r.err *MUST* be checked
func (r *reader) Next() bool {
	log.Println("Next()")
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if r.err != nil {
		return false // post-conditions are guaranteed because r.rec == nil && r.err != nil
	}
	// PROP I: r.rec == nil && r.err == nil

	log.Println("checking if activeChunk is nil")
	// if we don't have an active chunk, we need to wait for the loading one
	// parse it, and trigger a request for the next chunk to preserve
	// invariants I and II
	if r.activeChunk == nil {
		if r.loadingChunkIdx == -1 {
			return false // post-conditions are guaranteed by PROP I
		}
		log.Println("waiting for loading chunk")
		// wait for the loading chunk
		start := time.Now()
		chunk := <-r.chunkChan
		log.Printf("blocked %s waiting for chunk %d", time.Since(start).String(), chunk.chunkIndex)
		if chunk.err != nil {
			r.err = chunk.err
			close(r.chunkChan)
			return false // post-conditions are guaranteed by PROP I
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
			return false
		}
		r.activeChunk = array.RecordReader(chunkReader)
		r.activeChunk.Retain()
		// make sure r.schema is set when the first chunk is parsed
		if r.schema != nil {
			r.schema = r.activeChunk.Schema()
		}
	}
	// PROP II: r.activeChunk != nil

	log.Printf("calling Next on the active chunk")
	if r.activeChunk.Next() {
		r.rec = r.activeChunk.Record()
		r.rec.Retain()
		log.Printf("Success! Batch with %d rows\n", r.rec.NumRows())
		return true // post-conditions preserved since r.rec != nil
	}
	// make sure the error (if it exists) is retained
	r.err = r.activeChunk.Err()
	// release the fully consumed chunk and close the underlying HTTP body stream
	r.activeChunk.Release()
	r.activeChunk = nil
	r.activeChunkBody.Close()
	// PROP III: r.activeChunk == nil
	// recursively call Next() to start processing another chunk or stopping
	return r.Next()
}

func (r *reader) Schema() *arrow.Schema {
	if r.schema == nil {
		if r.activeChunk == nil {
			if r.loadingChunkIdx == -1 {
				return nil // XXX
			}
			log.Println("waiting for loading chunk")
			// wait for the loading chunk
			start := time.Now()
			chunk := <-r.chunkChan
			log.Printf("blocked %s waiting for chunk %d", time.Since(start).String(), chunk.chunkIndex)
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
			log.Println("parsing new chunk into a record reader")
			// parse the new chunk into a record reader
			r.activeChunkBody = chunk.inner.Body
			chunkReader, err := ipc.NewReader(r.activeChunkBody)
			if err != nil {
				log.Printf("error creating new chunk reader: %v", err)
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

func (r *reader) Record() arrow.Record {
	if r.err != nil {
		log.Fatalf("Record() called when there is an error: %v", r.err)
	}
	if r.rec == nil {
		log.Fatalf("Record() called when there is no record available")
	}
	return r.rec
}

func (r *reader) Err() error {
	return r.err
}

// Start an HTTP request for the chunk data and notify the chunkReceived channel
// when a response is received and data is available for streaming.
func (r *reader) startChunkDataRequest(chunkIndex int, externalLink *sql.ExternalLink) {
	url := ""
	if externalLink != nil {
		url = externalLink.ExternalLink
	} else {
		log.Printf("chunk %d: starting request for external link", chunkIndex)
		// TODO: retry logic
		req := sql.GetStatementResultChunkNRequest{
			ChunkIndex:  chunkIndex,
			StatementId: r.StatementId,
		}
		res, err := (*r.stmtExecution).GetStatementResultChunkN(context.TODO(), req)
		if err != nil {
			log.Printf("chunk %d: error getting external link: %v", chunkIndex, err)
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
	log.Printf("chunk %d: starting request chunk data %s", chunkIndex, url)
	res, err := r.httpClient.Get(url)
	r.chunkChan <- chunkResponse{
		chunkIndex: chunkIndex,
		inner:      res,
		err:        err,
	}
	// receiver is responsible for calling chunkResponse.Close()
}
