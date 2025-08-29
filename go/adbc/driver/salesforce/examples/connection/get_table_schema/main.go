/*
Salesforce ADBC GetTableSchema example.

Required environment variables:
- SALESFORCE_CLIENT_ID
- SALESFORCE_USERNAME
- SALESFORCE_PRIVATE_KEY_PATH

Usage: go run main.go
*/
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/examples/shared"
)

func main() {
	fmt.Println("Salesforce ADBC GetTableSchema Example")

	ctx := context.Background()

	// Create connection using shared utility
	conn, cleanup, err := shared.CreateSalesforceConnection(ctx)
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer cleanup()

	demonstrateGetTableSchema(ctx, conn)
}

func demonstrateGetTableSchema(ctx context.Context, conn adbc.Connection) {
	tableName := "CurrencyType_Home__dll"
	fmt.Printf("\nGetting schema for: %s\n", tableName)

	schema, err := conn.GetTableSchema(ctx, nil, nil, tableName)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("Fields: %d\n", len(schema.Fields()))
	for _, field := range schema.Fields() {
		nullable := ""
		if field.Nullable {
			nullable = " (nullable)"
		}
		fmt.Printf("  %s: %s%s\n", field.Name, field.Type, nullable)
	}

	// Test with dataspace
	fmt.Printf("\nTesting with dataspace 'default'...\n")
	dataspace := "default"
	_, err = conn.GetTableSchema(ctx, &dataspace, nil, tableName)
	if err != nil {
		fmt.Printf("Error with dataspace: %v\n", err)
	} else {
		fmt.Printf("Success with dataspace\n")
	}

	fmt.Println("\nDone!")
}
