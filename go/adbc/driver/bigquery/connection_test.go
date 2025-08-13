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

func TestBuildField(t *testing.T) {
	tests := []struct {
		name            string
		schema          *bigquery.FieldSchema
		expectedTypeStr string
		expectError     bool
	}{
		{
			name: "ArrayOfScalar",
			schema: &bigquery.FieldSchema{
				Name:        "test_array_scalar_field",
				Type:        bigquery.IntegerFieldType,
				Repeated:    true,
				Required:    false,
				Description: "Test array field with scalar type",
				Schema:      nil,
			},
			expectedTypeStr: "list<item: int64, nullable>",
			expectError:     false,
		},
		{
			name: "ArrayOfRecordWithMultipleFields",
			schema: &bigquery.FieldSchema{
				Name:        "test_array_field",
				Type:        bigquery.RecordFieldType,
				Repeated:    true,
				Required:    false,
				Description: "Test array field with multiple nested fields",
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "field1",
						Type:     bigquery.StringFieldType,
						Required: false,
					},
					{
						Name:     "field2",
						Type:     bigquery.IntegerFieldType,
						Required: false,
					},
				},
			},
			expectedTypeStr: "list<item: struct<field1: utf8, field2: int64>, nullable>",
			expectError:     false,
		},
		{
			name: "ArrayOfRecordWithSingleField",
			schema: &bigquery.FieldSchema{
				Name:        "test_single_array_field",
				Type:        bigquery.RecordFieldType,
				Repeated:    true,
				Required:    false,
				Description: "Test array field with single nested field",
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "single_field",
						Type:     bigquery.StringFieldType,
						Required: false,
					},
				},
			},
			expectedTypeStr: "list<item: struct<single_field: utf8>, nullable>",
			expectError:     false,
		},
		{
			name: "NonRepeatedRecord",
			schema: &bigquery.FieldSchema{
				Name:        "test_struct_field",
				Type:        bigquery.RecordFieldType,
				Repeated:    false,
				Required:    false,
				Description: "Test struct field with multiple nested fields",
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "nested_string",
						Type:     bigquery.StringFieldType,
						Required: false,
					},
					{
						Name:     "nested_int",
						Type:     bigquery.IntegerFieldType,
						Required: true,
					},
				},
			},
			expectedTypeStr: "struct<nested_string: utf8, nested_int: int64>",
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := buildField(tt.schema, 0)

			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error for test case %s, but got nil", tt.name)
				}
				return
			}

			if err != nil {
				t.Fatalf("Expected no error for test case %s, got: %v", tt.name, err)
			}

			if field.Name != tt.schema.Name {
				t.Errorf("Expected field name '%s', got '%s'", tt.schema.Name, field.Name)
			}

			typeStr := field.Type.String()
			if typeStr != tt.expectedTypeStr {
				t.Errorf("Expected field type string to be '%s', got '%s'", tt.expectedTypeStr, typeStr)
			}
		})
	}
}
