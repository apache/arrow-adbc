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

package flightsql

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/metadata"
)

type timeoutOption struct {
	grpc.EmptyCallOption

	// timeout for DoGet requests
	fetchTimeout time.Duration
	// timeout for GetFlightInfo requests
	queryTimeout time.Duration
	// timeout for DoPut or DoAction requests
	updateTimeout time.Duration
	// timeout for establishing a new connection
	connectTimeout time.Duration
}

func (t *timeoutOption) setTimeout(key string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return adbc.Error{
			Msg: fmt.Sprintf("[Flight SQL] invalid timeout option value %s = %f: timeouts must be non-negative and finite",
				key, value),
			Code: adbc.StatusInvalidArgument,
		}
	}

	timeout := time.Duration(value * float64(time.Second))

	switch key {
	case OptionTimeoutFetch:
		t.fetchTimeout = timeout
	case OptionTimeoutQuery:
		t.queryTimeout = timeout
	case OptionTimeoutUpdate:
		t.updateTimeout = timeout
	case OptionTimeoutConnect:
		t.connectTimeout = timeout
	default:
		return adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] Unknown timeout option '%s'", key),
			Code: adbc.StatusNotImplemented,
		}
	}
	return nil
}

func (t *timeoutOption) setTimeoutString(key string, value string) error {
	timeout, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return adbc.Error{
			Msg: fmt.Sprintf("[Flight SQL] invalid timeout option value %s = %s: %s",
				key, value, err.Error()),
			Code: adbc.StatusInvalidArgument,
		}
	}
	return t.setTimeout(key, timeout)
}

func (t *timeoutOption) connectParams() grpc.ConnectParams {
	return grpc.ConnectParams{
		Backoff:           backoff.DefaultConfig,
		MinConnectTimeout: t.connectTimeout,
	}
}

func getTimeout(method string, callOptions []grpc.CallOption) (time.Duration, bool) {
	for _, opt := range callOptions {
		if to, ok := opt.(timeoutOption); ok {
			var tm time.Duration
			switch {
			case strings.HasSuffix(method, "DoGet"):
				tm = to.fetchTimeout
			case strings.HasSuffix(method, "GetFlightInfo"):
				tm = to.queryTimeout
			case strings.HasSuffix(method, "DoPut") || strings.HasSuffix(method, "DoAction"):
				tm = to.updateTimeout
			}

			return tm, tm > 0
		}
	}

	return 0, false
}

func unaryTimeoutInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if tm, ok := getTimeout(method, opts); ok {
		ctx, cancel := context.WithTimeout(ctx, tm)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

type streamEventType int

const (
	receiveEndEvent streamEventType = iota
	errorEvent
)

type streamEvent struct {
	Type streamEventType
	Err  error
}

type wrappedClientStream struct {
	grpc.ClientStream

	desc       *grpc.StreamDesc
	events     chan streamEvent
	eventsDone chan struct{}
}

func (w *wrappedClientStream) RecvMsg(m any) error {
	err := w.ClientStream.RecvMsg(m)

	switch {
	case err == nil && !w.desc.ServerStreams:
		w.sendStreamEvent(receiveEndEvent, nil)
	case err == io.EOF:
		w.sendStreamEvent(receiveEndEvent, nil)
	case err != nil:
		w.sendStreamEvent(errorEvent, err)
	}

	return err
}

func (w *wrappedClientStream) SendMsg(m any) error {
	err := w.ClientStream.SendMsg(m)
	if err != nil {
		w.sendStreamEvent(errorEvent, err)
	}
	return err
}

func (w *wrappedClientStream) Header() (metadata.MD, error) {
	md, err := w.ClientStream.Header()
	if err != nil {
		w.sendStreamEvent(errorEvent, err)
	}
	return md, err
}

func (w *wrappedClientStream) CloseSend() error {
	err := w.ClientStream.CloseSend()
	if err != nil {
		w.sendStreamEvent(errorEvent, err)
	}
	return err
}

func (w *wrappedClientStream) sendStreamEvent(eventType streamEventType, err error) {
	select {
	case <-w.eventsDone:
	case w.events <- streamEvent{Type: eventType, Err: err}:
	}
}

func streamTimeoutInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if tm, ok := getTimeout(method, opts); ok {
		ctx, cancel := context.WithTimeout(ctx, tm)
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			defer cancel()
			return s, err
		}

		events, eventsDone := make(chan streamEvent), make(chan struct{})
		go func() {
			defer close(eventsDone)
			defer cancel()

			for {
				select {
				case event := <-events:
					// split by event type in case we want to add more logging
					// or even adding in some telemetry in the future.
					// Errors will already be propagated by the RecvMsg, SendMsg
					// methods.
					switch event.Type {
					case receiveEndEvent:
						return
					case errorEvent:
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		stream := &wrappedClientStream{
			ClientStream: s,
			desc:         desc,
			events:       events,
			eventsDone:   eventsDone,
		}
		return stream, nil
	}

	return streamer(ctx, desc, cc, method, opts...)
}
