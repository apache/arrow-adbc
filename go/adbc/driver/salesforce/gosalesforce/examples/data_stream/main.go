package main

import (
	"context"
	"fmt"
	"log"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/shared"
)

func main() {
	fmt.Println("Salesforce Data Cloud - Data Stream Examples")
	fmt.Println("=============================================")

	fmt.Println("\n=== JWT Authentication ===")
	client, err := api.NewClientWithJWT()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}

	fmt.Println("\n=== Data Stream API Examples ===")
	demonstrateDataStream(client)
}

func demonstrateDataStream(client *api.Client) {
	ctx := context.Background()
	dataStreamName := "customerscsv__dll"

	includeMappings := true
	dataStream, err := client.GetDataStream(ctx, dataStreamName, &includeMappings)
	if err != nil {
		fmt.Printf("❌ Failed to get data stream: %v\n", err)
		return
	}

	fmt.Printf("✅ Successfully retrieved data stream: %s\n", dataStreamName)
	shared.PrettyPrintJSON(dataStream)
}
