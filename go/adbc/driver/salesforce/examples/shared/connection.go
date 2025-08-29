// Package shared provides shared utilities for Salesforce ADBC examples
package shared

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GetEnvOrPanic gets an environment variable or panics with a helpful message
func GetEnvOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required. Please set it before running this example.", key)
	}
	return value
}

// CreateSalesforceConnection creates a Salesforce ADBC connection using JWT authentication
func CreateSalesforceConnection(ctx context.Context) (adbc.Connection, func(), error) {
	// Read private key
	privateKeyPath := GetEnvOrPanic("SALESFORCE_PRIVATE_KEY_PATH")
	if _, err := os.Stat(privateKeyPath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("private key file not found: %s", privateKeyPath)
	}

	privateKey, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read private key: %v", err)
	}

	// Create connection
	driver := salesforce.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		salesforce.OptionStringAuthType:      salesforce.OptionValueAuthTypeJwtBearer,
		salesforce.OptionStringClientID:      GetEnvOrPanic("SALESFORCE_CLIENT_ID"),
		salesforce.OptionStringUsername:      GetEnvOrPanic("SALESFORCE_USERNAME"),
		salesforce.OptionStringJWTPrivateKey: string(privateKey),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create database: %v", err)
	}

	fmt.Println("Connecting...")
	conn, err := db.Open(ctx)
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("connection failed: %v", err)
	}

	fmt.Println("Connected!")

	// Return connection and cleanup function
	cleanup := func() {
		conn.Close()
		db.Close()
	}

	return conn, cleanup, nil
}
