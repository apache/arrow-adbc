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

package utils

import "github.com/apache/arrow-go/v18/arrow"

func RemoveSchemaMetadata(schema *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, len(schema.Fields()))
	for i, field := range schema.Fields() {
		fields[i] = removeFieldMetadata(&field)
	}
	return arrow.NewSchema(fields, nil)
}

func removeFieldMetadata(field *arrow.Field) arrow.Field {
	fieldType := field.Type

	if nestedType, ok := field.Type.(arrow.NestedType); ok {
		childFields := make([]arrow.Field, len(nestedType.Fields()))
		for i, field := range nestedType.Fields() {
			childFields[i] = removeFieldMetadata(&field)
		}

		switch ty := field.Type.(type) {
		case *arrow.DenseUnionType:
			fieldType = arrow.DenseUnionOf(childFields, ty.TypeCodes())
		case *arrow.FixedSizeListType:
			fieldType = arrow.FixedSizeListOfField(ty.Len(), childFields[0])
		case *arrow.ListType:
			fieldType = arrow.ListOfField(childFields[0])
		case *arrow.LargeListType:
			fieldType = arrow.LargeListOfField(childFields[0])
		case *arrow.MapType:
			// XXX: arrow-go doesn't let us build a map type from fields (so
			// nonstandard field names or nullability will be lost here)

			// child must be struct
			structType := ty.Elem().(*arrow.StructType)
			// struct must have two children
			keyType := structType.Field(0).Type
			itemType := structType.Field(1).Type
			mapType := arrow.MapOf(keyType, itemType)
			mapType.KeysSorted = ty.KeysSorted
			fieldType = mapType
		case *arrow.SparseUnionType:
			fieldType = arrow.SparseUnionOf(childFields, ty.TypeCodes())
		case *arrow.StructType:
			fieldType = arrow.StructOf(childFields...)
		default:
			// XXX: ignore it
		}
	}

	return arrow.Field{
		Name:     field.Name,
		Type:     fieldType,
		Nullable: field.Nullable,
		Metadata: arrow.Metadata{},
	}
}
