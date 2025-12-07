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
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	dbsql "github.com/databricks/databricks-sql-go"
)

const (
	DEFAULT_PORT           = 443
	DEFAULT_RETRY_WAIT_MIN = 1 * time.Second
	DEFAULT_RETRY_WAIT_MAX = 30 * time.Second
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	// Connection Pool
	db           *sql.DB
	needsRefresh bool // Whether we need to re-initialize

	// Connection parameters
	uri            string
	serverHostname string
	httpPath       string
	accessToken    string
	port           int
	catalog        string
	schema         string

	// Query options
	queryTimeout        time.Duration
	maxRows             int
	queryRetryCount     int
	downloadThreadCount int

	// TLS/SSL options
	sslMode     string
	sslRootCert string
	sslCertPool *x509.CertPool
	sslInsecure bool

	// OAuth options (for future expansion)
	oauthClientID     string
	oauthClientSecret string
	oauthRefreshToken string
}

func (d *databaseImpl) resolveConnectionOptions() ([]dbsql.ConnOption, error) {
	if d.serverHostname == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "server hostname is required",
		}
	}

	if d.httpPath == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "HTTP path is required",
		}
	}

	// FIXME: Support other auth methods
	if d.accessToken == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "access token is required",
		}
	}

	opts := []dbsql.ConnOption{
		dbsql.WithAccessToken(d.accessToken),
		dbsql.WithServerHostname(d.serverHostname),
		dbsql.WithHTTPPath(d.httpPath),
	}

	// Validate and set custom port
	// Defaults to 443
	if d.port != 0 {
		opts = append(opts, dbsql.WithPort(d.port))
	} else {
		opts = append(opts, dbsql.WithPort(DEFAULT_PORT))
	}

	// Default namespace for queries (catalog/schema)
	if d.catalog != "" || d.schema != "" {
		opts = append(opts, dbsql.WithInitialNamespace(d.catalog, d.schema))
	}

	if d.queryTimeout > 0 {
		opts = append(opts, dbsql.WithTimeout(d.queryTimeout))
	}

	if d.maxRows > 0 {
		opts = append(opts, dbsql.WithMaxRows(int(d.maxRows)))
	}
	if d.queryRetryCount >= 0 {
		opts = append(opts, dbsql.WithRetries(d.queryRetryCount, DEFAULT_RETRY_WAIT_MIN, DEFAULT_RETRY_WAIT_MAX))
	}
	if d.downloadThreadCount > 0 {
		opts = append(opts, dbsql.WithMaxDownloadThreads(d.downloadThreadCount))
	}

	// TLS/SSL handling
	if d.sslCertPool != nil || d.sslInsecure {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if d.sslCertPool != nil {
			tlsConfig.RootCAs = d.sslCertPool
		}

		if d.sslInsecure {
			tlsConfig.InsecureSkipVerify = true
		}

		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		opts = append(opts, dbsql.WithTransport(transport))
	}

	return opts, nil
}

func (d *databaseImpl) initializeConnectionPool(ctx context.Context) (*sql.DB, error) {
	var db *sql.DB

	// Use URI if provided
	if d.uri != "" {
		var err error
		db, err = sql.Open("databricks", d.uri)
		if err != nil {
			return nil, err
		}
	} else {
		opts, err := d.resolveConnectionOptions()
		if err != nil {
			return nil, err
		}

		connector, err := dbsql.NewConnector(opts...)
		if err != nil {
			return nil, err
		}

		db = sql.OpenDB(connector)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		err = errors.Join(db.Close())
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to ping database: %v", err),
		}
	}

	return db, nil
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	// Re-initialize the connection pool and settings if anything
	// has changed, or we have not initialized yet
	if d.needsRefresh || d.db == nil {
		db, err := d.initializeConnectionPool(ctx)

		if err != nil {
			return nil, err
		}

		// Close the existing connection pool
		if d.db != nil {
			err = d.db.Close()
			if err != nil {
				return nil, err
			}
		}

		d.db = db
	}

	c, err := d.db.Conn(ctx)

	if err != nil {
		return nil, err
	}

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		catalog:            d.catalog,
		dbSchema:           d.schema,
		conn:               c,
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) Close() error {
	defer func() {
		d.needsRefresh = true
		d.db = nil
	}()
	return d.db.Close()
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyURI:
		return d.uri, nil
	case OptionServerHostname:
		return d.serverHostname, nil
	case OptionHTTPPath:
		return d.httpPath, nil
	case OptionAccessToken:
		return d.accessToken, nil
	case OptionPort:
		return strconv.Itoa(d.port), nil
	case OptionCatalog:
		return d.catalog, nil
	case OptionSchema:
		return d.schema, nil
	case OptionQueryTimeout:
		if d.queryTimeout > 0 {
			return d.queryTimeout.String(), nil
		}
		return "", nil
	case OptionMaxRows:
		if d.maxRows > 0 {
			return strconv.Itoa(d.maxRows), nil
		}
		return "", nil
	case OptionQueryRetryCount:
		if d.queryRetryCount > 0 {
			return strconv.Itoa(d.queryRetryCount), nil
		}
		return "", nil
	case OptionDownloadThreadCount:
		if d.downloadThreadCount > 0 {
			return strconv.Itoa(d.downloadThreadCount), nil
		}
		return "", nil
	case OptionSSLMode:
		return d.sslMode, nil
	case OptionSSLRootCert:
		return d.sslRootCert, nil
	case OptionOAuthClientID:
		return d.oauthClientID, nil
	case OptionOAuthClientSecret:
		return d.oauthClientSecret, nil
	case OptionOAuthRefreshToken:
		return d.oauthRefreshToken, nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	// We need to re-initialize the db/connection pool if options change
	d.needsRefresh = true
	for k, v := range options {
		err := d.SetOption(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) SetOption(key, value string) error {
	// We need to re-initialize the db/connection pool if options change
	d.needsRefresh = true
	switch key {
	case adbc.OptionKeyURI:
		// Strip the databricks:// scheme since databricks-sql-go expects raw DSN format
		if strings.HasPrefix(value, "databricks://") {
			d.uri = strings.TrimPrefix(value, "databricks://")
		} else {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid URI scheme: expected 'databricks://', got '%s'", value),
			}
		}
	case OptionServerHostname:
		d.serverHostname = value
	case OptionHTTPPath:
		d.httpPath = value
	case OptionAccessToken:
		d.accessToken = value
	case OptionPort:
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "invalid port number",
			}
		}
		d.port = port
	case OptionCatalog:
		d.catalog = value
	case OptionSchema:
		d.schema = value
	case OptionQueryTimeout:
		if value != "" {
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid query timeout: %v", err),
				}
			}
			d.queryTimeout = timeout
		}
	case OptionMaxRows:
		if value != "" {
			maxRows, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid max rows: %v", err),
				}
			}
			d.maxRows = maxRows
		}
	case OptionQueryRetryCount:
		if value != "" {
			retryCount, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid query retry count: %v", err),
				}
			}
			d.queryRetryCount = retryCount
		}
	case OptionDownloadThreadCount:
		if value != "" {
			threadCount, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid download thread count: %v", err),
				}
			}
			d.downloadThreadCount = threadCount
		}
	case OptionSSLMode:
		if value != "" {
			lowerValue := strings.ToLower(value)
			switch lowerValue {
			case "insecure":
				d.sslMode = lowerValue
				d.sslInsecure = true
			case "require":
				d.sslMode = lowerValue
				d.sslInsecure = false
			default:
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid SSL mode: %s (supported: 'require', 'insecure')", value),
				}
			}
		} else {
			d.sslMode = value
			d.sslInsecure = false
		}
	case OptionSSLRootCert:
		if value != "" {
			// Validate that the certificate file exists and can be read.
			// Then, store valid cert.

			caCert, err := os.ReadFile(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("failed to read SSL root certificate: %v", err),
				}
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "failed to parse SSL root certificate",
				}
			}

			d.sslRootCert = value
			d.sslCertPool = caCertPool
		} else {
			d.sslRootCert = value
			d.sslCertPool = nil
		}
	case OptionOAuthClientID:
		d.oauthClientID = value
	case OptionOAuthClientSecret:
		d.oauthClientSecret = value
	case OptionOAuthRefreshToken:
		d.oauthRefreshToken = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
