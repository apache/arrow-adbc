package api

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/suite"
)

type DLOSuite struct {
	APISuite
}

func (s *DLOSuite) TestCreateAndGetAndDeleteDLO() {
	ctx := context.Background()
	dloName := "sftest_dlo__dll"

	// Clean up any leftover from previous runs
	_ = s.Client.DeleteDataLakeObject(ctx, dloName)

	req := &types.DataLakeObjectRequest{
		Name:     dloName,
		Label:    "sftest DLO",
		Category: types.CategoryProfile,
		FieldInputRepresentations: []types.DataLakeField{
			{Name: "id__c", Label: "id__c", DataType: "Text", IsPrimaryKey: true},
			{Name: "name__c", Label: "name__c", DataType: "Text", IsPrimaryKey: false},
		},
	}

	dlo, err := s.Client.CreateDataLakeObject(ctx, req)
	if err != nil {
		// May fail if previous run's DLO is still processing
		sfErr, ok := err.(*SalesforceError)
		if ok && sfErr.Code == "INVALID_INPUT" {
			s.T().Skipf("DLO still exists from previous run (likely still processing): %v", err)
		}
		s.Require().NoError(err, "create DLO failed")
	}
	s.T().Logf("Created DLO: %s (id=%s, status=%s)", dlo.Name, dlo.ID, dlo.Status)

	// Get
	fetched, err := s.Client.GetDataLakeObject(ctx, dloName)
	s.Require().NoError(err, "get DLO failed")
	s.Equal(dloName, fetched.Name)
	s.T().Logf("Fetched DLO: %s (status=%s)", fetched.Name, fetched.Status)

	// Delete — may fail if DLO is still processing (async provisioning)
	err = s.Client.DeleteDataLakeObject(ctx, dloName)
	if err != nil {
		s.T().Logf("Delete DLO deferred (still processing): %v", err)
	} else {
		s.T().Log("Deleted DLO successfully")
	}
}

func TestDLOSuite(t *testing.T) {
	suite.Run(t, new(DLOSuite))
}
