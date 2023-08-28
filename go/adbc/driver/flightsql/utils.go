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

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	// slice of proto.Message or error
	for _, detail := range grpcStatus.Details() {
		if err, ok := detail.(error); ok {
			details = append(details, &adbc.TextErrorDetail{Name: "grpc-status-details-bin", Detail: err.Error()})
		} else if msg, ok := detail.(proto.Message); ok {
			details = append(details, &adbc.ProtobufErrorDetail{Name: "grpc-status-details-bin", Message: msg})
		}
		// else, gRPC returned non-Protobuf detail in violation of their method contract
	}

	// XXX(https://github.com/grpc/grpc-go/issues/5485): don't count on
	// grpc-status-details-bin since Google hardcodes it to only work with
	// Google Cloud
	// XXX: must check both headers and trailers because some implementations
	// (like gRPC-Java) will consolidate trailers into headers for failed RPCs
	for key, values := range header {
		switch key {
		case "content-type", "grpc-status-details-bin":
			continue
		default:
			for _, value := range values {
				details = append(details, &adbc.TextErrorDetail{Name: key, Detail: value})
			}
		}
	}
	for key, values := range trailer {
		switch key {
		case "content-type", "grpc-status-details-bin":
			continue
		default:
			for _, value := range values {
				details = append(details, &adbc.TextErrorDetail{Name: key, Detail: value})
			}
		}
	}

	return adbc.Error{
		// People don't read error messages, so backload the context and frontload the server error
		Msg:     fmt.Sprintf("[FlightSQL] %s (%s; %s)", grpcStatus.Message(), grpcStatus.Code(), fmt.Sprintf(context, args...)),
		Code:    adbcCode,
		Details: details,
	}
}

func checkContext(maybeErr error, ctx context.Context) error {
	if maybeErr != nil {
		return maybeErr
	} else if ctx.Err() == context.Canceled {
		return adbc.Error{Msg: "Cancelled by request", Code: adbc.StatusCancelled}
	} else if ctx.Err() == context.DeadlineExceeded {
		return adbc.Error{Msg: "Deadline exceeded", Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}
