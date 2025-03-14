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
