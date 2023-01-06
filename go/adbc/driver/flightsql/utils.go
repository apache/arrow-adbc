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
	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func adbcFromFlightStatus(err error) error {
	var adbcCode adbc.Status
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.Canceled:
		adbcCode = adbc.StatusCancelled
	case codes.Unknown:
		adbcCode = adbc.StatusUnknown
	case codes.Internal:
		adbcCode = adbc.StatusInternal
	case codes.InvalidArgument:
		adbcCode = adbc.StatusInvalidArgument
	case codes.AlreadyExists:
		adbcCode = adbc.StatusAlreadyExists
	case codes.NotFound:
		adbcCode = adbc.StatusNotFound
	case codes.Unauthenticated:
		adbcCode = adbc.StatusUnauthenticated
	case codes.Unavailable:
		adbcCode = adbc.StatusIO
	case codes.PermissionDenied:
		adbcCode = adbc.StatusUnauthorized
	default:
		adbcCode = adbc.StatusUnknown
	}

	return adbc.Error{
		Msg:  err.Error(),
		Code: adbcCode,
	}
}
