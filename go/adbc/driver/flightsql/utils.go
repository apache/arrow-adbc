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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func adbcFromFlightStatus(err error, context string, args ...any) error {
	var header, trailer metadata.MD
	return adbcFromFlightStatusWithDetails(err, header, trailer, context, args...)
}

func adbcFromFlightStatusWithDetails(err error, header, trailer metadata.MD, context string, args ...any) error {
	if _, ok := err.(adbc.Error); ok {
		return err
	}

	var adbcCode adbc.Status
	// If not a status.Status, will return codes.Unknown
	grpcStatus := status.Convert(err)
	switch grpcStatus.Code() {
	case codes.OK:
		return nil
	case codes.Canceled:
		adbcCode = adbc.StatusCancelled
	case codes.Unknown:
		adbcCode = adbc.StatusUnknown
	case codes.InvalidArgument:
		adbcCode = adbc.StatusInvalidArgument
	case codes.DeadlineExceeded:
		adbcCode = adbc.StatusTimeout
	case codes.NotFound:
		adbcCode = adbc.StatusNotFound
	case codes.AlreadyExists:
		adbcCode = adbc.StatusAlreadyExists
	case codes.PermissionDenied:
		adbcCode = adbc.StatusUnauthorized
	case codes.ResourceExhausted:
		adbcCode = adbc.StatusInternal
	case codes.FailedPrecondition:
		adbcCode = adbc.StatusUnknown
	case codes.Aborted:
		adbcCode = adbc.StatusUnknown
	case codes.OutOfRange:
		adbcCode = adbc.StatusUnknown
	case codes.Unimplemented:
		adbcCode = adbc.StatusNotImplemented
	case codes.Internal:
		adbcCode = adbc.StatusInternal
	case codes.Unavailable:
		adbcCode = adbc.StatusIO
	case codes.DataLoss:
		adbcCode = adbc.StatusIO
	case codes.Unauthenticated:
		adbcCode = adbc.StatusUnauthenticated
	default:
		adbcCode = adbc.StatusUnknown
	}

	details := []adbc.ErrorDetail{}
	for _, detail := range grpcStatus.Proto().Details {
		details = append(details, &anyErrorDetail{name: "grpc-status-details-bin", message: detail})
	}

	// XXX(https://github.com/grpc/grpc-go/issues/5485): don't count on
	// grpc-status-details-bin since Google hardcodes it to only work with
	// Google Cloud
	// XXX: must check both headers and trailers because some implementations
	// (like gRPC-Java) will consolidate trailers into headers for failed RPCs
	for key, values := range header {
		switch {
		case key == "content-type":
			// Not useful info
			continue
		case key == "grpc-status-details-bin":
			// gRPC library parses this above via grpcStatus.Proto()
			continue
		case strings.HasSuffix(key, "-bin"):
			for _, value := range values {
				// that's right, gRPC stuffs binary data into a "string"
				details = append(details, &adbc.BinaryErrorDetail{Name: key, Detail: []byte(value)})
			}
		default:
			for _, value := range values {
				details = append(details, &adbc.TextErrorDetail{Name: key, Detail: value})
			}
		}
	}
	for key, values := range trailer {
		switch {
		case key == "content-type":
			// Not useful info
			continue
		case key == "grpc-status-details-bin":
			// gRPC library parses this above via grpcStatus.Proto()
			continue
		case strings.HasSuffix(key, "-bin"):
			for _, value := range values {
				// that's right, gRPC stuffs binary data into a "string"
				details = append(details, &adbc.BinaryErrorDetail{Name: key, Detail: []byte(value)})
			}
		default:
			for _, value := range values {
				details = append(details, &adbc.TextErrorDetail{Name: key, Detail: value})
			}
		}
	}

	return adbc.Error{
		// People don't read error messages, so backload the context and frontload the server error
		Msg:        fmt.Sprintf("%s (%s; %s)", grpcStatus.Message(), grpcStatus.Code(), fmt.Sprintf(context, args...)),
		Code:       adbcCode,
		VendorCode: int32(grpcStatus.Code()),
		Details:    details,
	}
}

func checkContext(maybeErr error, ctx context.Context) error {
	if maybeErr != nil && !errors.Is(maybeErr, io.EOF) {
		return maybeErr
	} else if ctx.Err() == context.Canceled {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if ctx.Err() == context.DeadlineExceeded {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

// grpc's Status derps if you ask it to deserialize the error details, giving
// you an error for each item. Instead, poke into its internals and directly
// extract and return the protobuf Any to the client.
type anyErrorDetail struct {
	name    string
	message *anypb.Any
}

func (d *anyErrorDetail) Key() string {
	return d.name
}

// Serialize serializes the Protobuf message (wrapped in Any).
func (d *anyErrorDetail) Serialize() ([]byte, error) {
	return proto.Marshal(d.message)
}
