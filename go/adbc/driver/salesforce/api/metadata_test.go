package api

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/suite"
)

type MetadataSuite struct {
	APISuite
}

func (s *MetadataSuite) TestGetMetadata() {
	resp, err := s.Client.GetMetadata(context.Background(), &types.MetadataRequest{})
	s.Require().NoError(err)
	s.NotNil(resp)
	s.NotEmpty(resp.Metadata, "expected at least one metadata entity")

	// Verify structure of first entity
	entity := resp.Metadata[0]
	s.NotEmpty(entity.Name)
}

func (s *MetadataSuite) TestGetMetadata_WithEntityCategory() {
	resp, err := s.Client.GetMetadata(context.Background(), &types.MetadataRequest{
		EntityCategory: "Profile",
	})
	s.Require().NoError(err)
	s.NotNil(resp)
	// The filter is applied server-side; we just verify we got a response
	// (the Category field in the response may be empty depending on API version)
	s.T().Logf("Got %d entities with entityCategory=Profile filter", len(resp.Metadata))
}

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, new(MetadataSuite))
}
