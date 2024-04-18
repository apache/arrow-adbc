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

package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"google.golang.org/api/option"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	authType    string
	credentials *string
	projectID   *string
	datasetID   *string
	tableID     *string

	alloc memory.Allocator
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &ConnectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		database:           d,
		alloc:              d.alloc,
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		Connection(), nil
}

func (d *databaseImpl) Close() error {
	return nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAuthType:
		return d.authType, nil
	case OptionStringCredentials:
		if d.credentials == nil {
			return "", nil
		} else {
			return *d.credentials, nil
		}
	case OptionStringProjectID:
		if d.projectID == nil {
			return "", adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "ProjectID is not set",
			}
		}
		return *d.projectID, nil
	case OptionStringDatasetID:
		if d.datasetID == nil {
			return "", adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "DatasetID is not set",
			}
		}
		return *d.datasetID, nil
	case OptionStringTableID:
		if d.tableID == nil {
			return "", adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "TableID is not set",
			}
		}
		return *d.tableID, nil
	default:
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database string type option `%s`", key),
		}
	}
}

func (d *databaseImpl) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown database bytes type option `%s`", key),
	}
}

func (d *databaseImpl) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown database int type option `%s`", key),
	}
}

func (d *databaseImpl) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown database double type option `%s`", key),
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		err := d.SetOption(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case OptionStringAuthType:
		switch value {
		case OptionValueAuthTypeDefault:
			d.authType = value
		case OptionValueAuthTypeCredentialsFile:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown database auth type value `%s`", value),
			}
		}
	case OptionStringCredentials:
		d.credentials = &value
	case OptionStringProjectID:
		d.projectID = &value
	case OptionStringDatasetID:
		d.datasetID = &value
	case OptionStringTableID:
		d.tableID = &value
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database string type option `%s`", key),
		}
	}
	return nil
}

func (d *databaseImpl) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown database bytes type option `%s`", key),
	}
}

func (d *databaseImpl) SetOptionInt(key string, value int64) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown database int type option `%s`", key),
	}
}

func (d *databaseImpl) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown database double type option `%s`", key),
	}
}

func newClient(ctx context.Context, projectID, authType, credentials string) (*bigquery.Client, error) {
	switch authType {
	case OptionValueAuthTypeCredentialsFile:
		client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentials))
		if err != nil {
			return nil, err
		}
		err = client.EnableStorageReadClient(ctx, option.WithCredentialsFile(credentials))
		if err != nil {
			return nil, err
		}
		return client, nil
	default:
		client, err := bigquery.NewClient(ctx, projectID)
		if err != nil {
			return nil, err
		}
		err = client.EnableStorageReadClient(ctx)
		if err != nil {
			return nil, err
		}
		return client, nil
	}
}
