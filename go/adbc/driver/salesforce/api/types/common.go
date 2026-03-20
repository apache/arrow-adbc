package types

import (
	"strings"
	"time"
)

type DataCloudError struct {
	Code    string `json:"errorCode"`
	Message string `json:"errorMessage"`
}

// DataCloudActionResponse is the standard response for action endpoints.
type DataCloudActionResponse struct {
	Success bool             `json:"success"`
	Errors  []DataCloudError `json:"errors,omitempty"`
}

// Category is the category of a data object (DLO, DMO, OutputDataObject, etc.).
type Category string

const (
	CategoryProfile        Category = "Profile"
	CategoryEngagement     Category = "Engagement"
	CategoryOther          Category = "Other"
	CategoryInsights       Category = "Insights"        // unused
	CategoryDirectoryTable Category = "Directory_Table" // unused
)

type Status string

const (
	StatusActive     Status = "Active"
	StatusProcessing Status = "Processing"
	StatusError      Status = "Error"
	StatusInactive   Status = "Inactive"
	StatusDeleting   Status = "Deleting"
)

func (s Status) isEq(other Status) bool {
	return strings.EqualFold(string(s), string(other))
}

func (s Status) IsActive() bool     { return s.isEq(StatusActive) }
func (s Status) IsProcessing() bool { return s.isEq(StatusProcessing) }
func (s Status) IsError() bool      { return s.isEq(StatusError) }
func (s Status) IsInactive() bool   { return s.isEq(StatusInactive) }
func (s Status) IsDeleting() bool   { return s.isEq(StatusDeleting) }

func (s Status) IsOneOf(statuses ...Status) bool {
	for _, other := range statuses {
		if s.isEq(other) {
			return true
		}
	}
	return false
}

// Example usage:
//
//	type Dummy struct {
//		// ...
//	}
//
//	type DummyCollection struct {
//		Paginated                     // embedded without json tag
//		Items[Dummy] `json:"dummies"` // embedded with json tag
//	}
type Pagination interface {
	// TODO: add helper methods working with paginated collections
	// The idea is that this interface is automatically satisfied by any struct embedding [Paginated] and [Items] (e.g. DummyCollection in the example above).
	// This allows us to write helper functions that take in a Pagination and work with the common pagination fields, without needing to know the specific type of items in the collection.
	//
	// The methods here should be a union of the [Paginated] and [Items] methods.
}

// Paginated is meant to be embedded in a `...Collection` struct.
//
// These are the common fields for all paginated responses for Data 360 Connect.
// Should be used in combination with [Items] to create Collection types for each resource.
//
// See [Pagination] for usage.
type Paginated struct {
	// TotalSize of the collection.
	TotalSize int64 `json:"totalSize"`

	// CurrentPageURL.
	CurrentPageUrl string `json:"currentPageUrl"`

	// NextPageURL, if it exists.
	NextPageUrl string `json:"nextPageUrl"`
}

func (p *Paginated) HasNextPage() bool {
	return p.NextPageUrl != ""
}

// Items is meant to be embedded in a `...Collection` struct with a `json` tag.
//
// Data 360 Connect doesn't use a common key for the list of resources.
// Should be used in combination with [Paginated] to create Collection types for each resource.
//
// See [Pagination] for usage.
type Items[T any] []T

// TODO: create remaining collection types

// BackoffConfig controls exponential backoff behavior for polling operations.
type BackoffConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration // 0 = no limit, rely on ctx
	Multiplier      float64
	RandomFactor    float64
}

func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialInterval: 30 * time.Second,
		MaxInterval:     600 * time.Second,
		MaxElapsedTime:  0,
		Multiplier:      1.5,
		RandomFactor:    0.5,
	}
}
