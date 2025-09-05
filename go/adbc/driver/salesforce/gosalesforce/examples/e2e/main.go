package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
)

func main() {
	fmt.Println("Salesforce Data Cloud - Data Transform Examples")
	fmt.Println("===============================================")

	// auth
	client, err := api.NewClientWithJWT()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}
	ctx := context.Background()

	// setups
	targetDLOs := []string{"customers_child__dll", "customers_child_2__dll"}
	queries := []string{
		"SELECT \"CustomerId__c\" as \"CustomerId_child__c\" FROM \"customers_raw__dll\"",
		"SELECT \"CustomerId__c\" as \"CustomerId_child_2__c\" FROM \"customers_raw__dll\"",
	}

	// Create DLOs sequentially first
	var dataLakeObjects []*api.DataLakeObject
	fmt.Printf("Creating %d DLOs...\n", len(targetDLOs))

	for i, targetDLO := range targetDLOs {
		err = client.DeleteIfDloExists(ctx, targetDLO)
		if err != nil {
			fmt.Printf("ERROR: Failed to delete DLO %s: %v\n", targetDLO, err)
			return
		}

		primaryKeyField := fmt.Sprintf("CustomerId_child%s__c", map[int]string{0: "", 1: "_2"}[i])
		dataLakeObject, err := client.CreateDataLakeObjectWithInferredSchema(ctx, queries[i], "default", targetDLO, primaryKeyField, api.DataLakeObjectCategoryProfile)
		if err != nil {
			fmt.Printf("ERROR: Failed to create DLO %s: %v\n", targetDLO, err)
			return
		}

		dataLakeObjects = append(dataLakeObjects, dataLakeObject)
	}
	fmt.Printf("✅ All %d DLOs created successfully\n", len(dataLakeObjects))

	// Run data transforms in parallel
	fmt.Printf("\n🚀 Running %d data transforms in parallel...\n", len(dataLakeObjects))
	runDataTransformsInParallel(ctx, client, dataLakeObjects, queries)
}

// DataTransformResult holds the result of a data transform operation
type DataTransformResult struct {
	DLOName       string
	DataTransform *api.DataTransform
	Error         error
	Duration      time.Duration
	StartTime     time.Time
	EndTime       time.Time
}

// runDataTransformsInParallel executes TriggerDbtBatchDataTransform for multiple DLOs concurrently
func runDataTransformsInParallel(ctx context.Context, client *api.Client, dataLakeObjects []*api.DataLakeObject, queries []string) {
	var wg sync.WaitGroup
	results := make(chan DataTransformResult, len(dataLakeObjects))

	startTime := time.Now()
	// Launch goroutines for each data transform
	for i, dlo := range dataLakeObjects {
		wg.Add(1)
		go func(index int, dataLakeObject *api.DataLakeObject, query string) {
			defer wg.Done()

			startTime := time.Now()
			dataTransform, err := client.TriggerDbtBatchDataTransform(ctx, dataLakeObject, query, true)
			endTime := time.Now()
			duration := endTime.Sub(startTime)

			results <- DataTransformResult{
				DLOName:       dataLakeObject.Name,
				DataTransform: dataTransform,
				Error:         err,
				Duration:      duration,
				StartTime:     startTime,
				EndTime:       endTime,
			}
		}(i, dlo, queries[i])
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and display results
	var totalDurationActual time.Duration = time.Since(startTime)

	successCount := 0
	errorCount := 0
	var totalDurationAccumulated time.Duration
	var minDuration, maxDuration time.Duration
	var allResults []DataTransformResult

	for result := range results {
		allResults = append(allResults, result)
		totalDurationAccumulated += result.Duration

		if minDuration == 0 || result.Duration < minDuration {
			minDuration = result.Duration
		}
		if result.Duration > maxDuration {
			maxDuration = result.Duration
		}

		if result.Error != nil {
			fmt.Printf("❌ %s failed: %v\n", result.DLOName, result.Error)
			errorCount++
		} else {
			fmt.Printf("✅ %s completed in %v\n", result.DLOName, result.Duration.Round(time.Millisecond))
			successCount++
		}
	}

	// Final summary
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total: %d | ✅ Success: %d | ❌ Failed: %d\n", len(dataLakeObjects), successCount, errorCount)

	if len(allResults) > 0 {
		avgDuration := totalDurationAccumulated / time.Duration(len(allResults))
		fmt.Printf("⏱️  Parallel execution: %v | Average per transform: %v\n",
			totalDurationActual.Round(time.Millisecond), avgDuration.Round(time.Millisecond))
		fmt.Printf("⏱️  Range: %v (fastest) - %v (slowest)\n",
			minDuration.Round(time.Millisecond), maxDuration.Round(time.Millisecond))
	}

	if errorCount == 0 {
		fmt.Println("🎉 All data transforms completed successfully!")
	} else {
		fmt.Printf("⚠️  %d transforms failed\n", errorCount)
	}
}
