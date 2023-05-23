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
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func adbcFromFlightStatus(err error) error {
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
		} else {
			panic(fmt.Sprintf("gRPC returned non-Protobuf detail in violation of method contract: %#v", detail))
		}
	}

	return adbc.Error{
		Msg:     err.Error(),
		Code:    adbcCode,
		Details: details,
	}
}
