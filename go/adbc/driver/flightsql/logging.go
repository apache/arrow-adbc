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
	"io"
	"log/slog"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func makeUnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		// Ignore errors
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		err := invoker(ctx, method, req, reply, cc, opts...)
		if logger.Enabled(ctx, slog.LevelDebug) {
			logger.DebugContext(ctx, method, "target", cc.Target(), "duration", time.Since(start), "err", err, "metadata", outgoing)
		} else {
			keys := maps.Keys(outgoing)
			slices.Sort(keys)
			logger.InfoContext(ctx, method, "target", cc.Target(), "duration", time.Since(start), "err", err, "metadata", keys)
		}
		return err
	}
	return interceptor
}

func makeStreamLoggingInterceptor(logger *slog.Logger) grpc.StreamClientInterceptor {
	interceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		// Ignore errors
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			logger.InfoContext(ctx, method, "target", cc.Target(), "duration", time.Since(start), "err", err)
			return stream, err
		}

		return &loggedStream{ClientStream: stream, logger: logger, ctx: ctx, method: method, start: start, target: cc.Target(), outgoing: outgoing}, err
	}
	return interceptor
}

type loggedStream struct {
	grpc.ClientStream

	logger   *slog.Logger
	ctx      context.Context
	method   string
	start    time.Time
	target   string
	outgoing metadata.MD
}

func (stream *loggedStream) RecvMsg(m any) error {
	err := stream.ClientStream.RecvMsg(m)
	if err != nil {
		loggedErr := err
		if loggedErr == io.EOF {
			loggedErr = nil
		}

		if stream.logger.Enabled(stream.ctx, slog.LevelDebug) {
			stream.logger.DebugContext(stream.ctx, stream.method, "target", stream.target, "duration", time.Since(stream.start), "err", loggedErr, "metadata", stream.outgoing)
		} else {
			keys := maps.Keys(stream.outgoing)
			slices.Sort(keys)
			stream.logger.InfoContext(stream.ctx, stream.method, "target", stream.target, "duration", time.Since(stream.start), "err", loggedErr, "metadata", keys)
		}
	}
	return err
}
