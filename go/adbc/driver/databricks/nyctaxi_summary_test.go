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

package databricks_test

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type TestConfig struct {
	Environment     string `json:"environment"`
	URI             string `json:"uri"`
	Token           string `json:"token"`
	Query           string `json:"query"`
	Type            string `json:"type"`
	Trace           string `json:"trace"`
	ExpectedResults int    `json:"expectedResults"`
	Metadata        struct {
		Catalog             string `json:"catalog"`
		Schema              string `json:"schema"`
		Table               string `json:"table"`
		ExpectedColumnCount int    `json:"expectedColumnCount"`
	} `json:"metadata"`
}

func loadTestConfig(configPath string) (*TestConfig, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config TestConfig
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func parseConnectionInfo(uri string) (hostname, httpPath string, err error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	hostname = parsed.Host
	httpPath = parsed.Path
	return hostname, httpPath, nil
}

// Helper function to format large numbers with commas
func formatNumber(n int64) string {
	str := ""
	num := n
	if num < 0 {
		str = "-"
		num = -num
	}

	digits := []rune{}
	for {
		digits = append([]rune{'0' + rune(num%10)}, digits...)
		num /= 10
		if num == 0 {
			break
		}
	}

	result := str
	for i, digit := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			result += ","
		}
		result += string(digit)
	}

	return result
}

func TestNYCTaxi_TotalRowCount(t *testing.T) {
	config, err := loadTestConfig("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfo(config.URI)
	require.NoError(t, err)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: hostname,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    config.Token,
		databricks.OptionCatalog:        "samples",
		databricks.OptionSchema:         "nyctaxi",
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get the exact row count
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	startTime := time.Now()
	err = stmt.SetSqlQuery("SELECT COUNT(*) AS total_rows FROM samples.nyctaxi.trips")
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	var totalRowCount int64

	if reader.Next() {
		record := reader.Record()
		require.Equal(t, int64(1), record.NumCols(), "Expected 1 column for COUNT(*)")
		require.Equal(t, int64(1), record.NumRows(), "Expected 1 row for COUNT(*)")

		// Extract the count value from the Arrow record
		col := record.Column(0)
		if int64Array, ok := col.(*array.Int64); ok {
			totalRowCount = int64Array.Value(0)
		} else {
			t.Fatalf("Expected int64 column, got %T", col)
		}
	}

	queryDuration := time.Since(startTime)

	// Log comprehensive results
	t.Logf("🎯 NYC Taxi Dataset Analysis Results:")
	t.Logf("📊 Total rows in samples.nyctaxi.trips: %s", formatNumber(totalRowCount))
	t.Logf("⏱️  COUNT(*) query execution time: %v", queryDuration)
	t.Logf("🚀 Query processing rate: %.2f rows/ms", float64(totalRowCount)/float64(queryDuration.Milliseconds()))

	// Verify we got a reasonable count
	require.Greater(t, totalRowCount, int64(0), "Expected positive row count")
	require.Greater(t, totalRowCount, int64(1000), "Expected substantial dataset (>1000 rows)")

	t.Logf("✅ Successfully read row count from %s rows using Databricks Go ADBC driver", formatNumber(totalRowCount))
}

func TestNYCTaxi_TableInfo(t *testing.T) {
	config, err := loadTestConfig("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfo(config.URI)
	require.NoError(t, err)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: hostname,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    config.Token,
		databricks.OptionCatalog:        "samples",
		databricks.OptionSchema:         "nyctaxi",
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get detailed table information
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	startTime := time.Now()
	err = stmt.SetSqlQuery("DESCRIBE TABLE samples.nyctaxi.trips")
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	t.Logf("📋 Table Schema for samples.nyctaxi.trips:")

	columnCount := 0
	for reader.Next() {
		record := reader.Record()
		schema := record.Schema()

		t.Logf("Schema metadata columns:")
		for i := 0; i < int(record.NumCols()); i++ {
			field := schema.Field(i)
			t.Logf("  Metadata column %d: %s (%s)", i, field.Name, field.Type.String())
		}

		columnCount += int(record.NumRows())
	}

	queryDuration := time.Since(startTime)
	t.Logf("✅ Table has %d columns", columnCount)
	t.Logf("⏱️  DESCRIBE TABLE execution time: %v", queryDuration)
}
