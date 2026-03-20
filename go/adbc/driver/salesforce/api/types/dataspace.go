package types

// DataSpaceMember represents a member of a data space.
type DataSpaceMember struct {
	Name   string        `json:"memberName"`
	Filter *FilterConfig `json:"filter,omitempty"`
}

// FilterConjunctiveOperator is the logical operator for combining filter conditions.
type FilterConjunctiveOperator = string

const (
	FilterConjunctiveAnd FilterConjunctiveOperator = "AndOperator"
	FilterConjunctiveOr  FilterConjunctiveOperator = "OrOperator"
)

// FilterOperator is the comparison operator for a filter condition.
type FilterOperator = string

const (
	FilterOperatorEquals FilterOperator = "EqualsOperator"
	FilterOperatorIn     FilterOperator = "InOperator"
)

// FilterConfig represents a filter configuration for data space members.
// TODO: needs confirmation through testing
type FilterConfig struct {
	ConjunctiveOperator FilterConjunctiveOperator `json:"conjunctiveOperator,omitempty"`
	Conditions          []FilterCondition          `json:"conditions,omitempty"`
}

// FilterCondition represents a single filter condition.
type FilterCondition struct {
	FieldName   string         `json:"fieldName"`
	Operator    FilterOperator `json:"operator"`
	FilterValue string         `json:"filterValue"`
}
