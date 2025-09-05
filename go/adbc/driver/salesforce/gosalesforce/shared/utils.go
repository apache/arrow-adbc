package shared

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func GetEnvOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required. Please set it before running this example.", key)
	}
	return value
}

func PrettyPrintJSON[T any](v T) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Printf("Failed to marshal object: %v\n", err)
		return
	}
	fmt.Println(string(b))
}
