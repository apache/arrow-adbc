package api

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ErrorsSuite struct {
	suite.Suite
}

func (s *ErrorsSuite) TestSalesforceError_Error() {
	err := &SalesforceError{StatusCode: 404, Code: "NOT_FOUND", Message: "Resource not found"}
	s.Equal("salesforce 404 NOT_FOUND: Resource not found", err.Error())
	s.True(err.IsNotFound())
	s.False(err.IsRateLimited())
}

func (s *ErrorsSuite) TestSalesforceError_ErrorWithoutCode() {
	err := &SalesforceError{StatusCode: 500, Message: "Internal server error"}
	s.Equal("salesforce 500: Internal server error", err.Error())
}

func TestErrorsSuite(t *testing.T) {
	suite.Run(t, new(ErrorsSuite))
}
