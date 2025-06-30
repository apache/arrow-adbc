// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestStringToTable(t *testing.T) {
	tests := []struct {
		name             string
		defaultProjectID string
		defaultDatasetID string
		input            string
		expected         *bigquery.Table
		expectError      bool
	}{
		{
			name:             "table only",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "mytable",
			expected: &bigquery.Table{
				ProjectID: "default-project",
				DatasetID: "default-dataset",
				TableID:   "mytable",
			},
			expectError: false,
		},
		{
			name:             "dataset and table",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "mydataset.mytable",
			expected: &bigquery.Table{
				ProjectID: "default-project",
				DatasetID: "mydataset",
				TableID:   "mytable",
			},
			expectError: false,
		},
		{
			name:             "project, dataset and table",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "myproject.mydataset.mytable",
			expected: &bigquery.Table{
				ProjectID: "myproject",
				DatasetID: "mydataset",
				TableID:   "mytable",
			},
			expectError: false,
		},
		{
			name:             "too many parts",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "a.b.c.d",
			expected:         nil,
			expectError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &statement{
				cnxn: &connectionImpl{
					catalog:  tt.defaultProjectID,
					dbSchema: tt.defaultDatasetID,
				},
			}
			result, err := stringToTable(st, tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result.ProjectID != tt.expected.ProjectID ||
				result.DatasetID != tt.expected.DatasetID ||
				result.TableID != tt.expected.TableID {
				t.Errorf("got %+v, want %+v", result, tt.expected)
			}
		})
	}
}
