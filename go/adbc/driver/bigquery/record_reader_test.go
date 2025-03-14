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

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestEmptyArrowIteratorNext(t *testing.T) {
	iter := emptyArrowIterator{}
	res, err := iter.Next()

	if res != nil {
		t.Errorf("Expected the result from Next to be nil, but got %v", res)
	}
	if err == nil {
		t.Errorf("Expected an error from Next, but got nil")
	}
}

func TestEmptyArrowIteratorSchema(t *testing.T) {
	iter := emptyArrowIterator{}
	schema := iter.Schema()

	if len(schema) > 0 {
		t.Errorf("Expected an empty schema, but got %d", len(schema))
	}
}

func TestEmptyArrowIteratorSerializedArrowSchema(t *testing.T) {
	iter := emptyArrowIterator{}
	bytes := iter.SerializedArrowSchema()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rdr, _ := ipcReaderFromArrowIterator(iter, alloc)
	if len(rdr.Schema().Fields()) > 0 {
		t.Errorf("Expected an empty schema, but got %d bytes", len(bytes))
	}
}
