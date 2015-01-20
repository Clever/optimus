package mongo

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/tests"
	"reflect"
	"testing"
)

// mongoIter is a "successful" iterator over some docs
type mongoIter struct {
	docs []interface{}
}

func (i *mongoIter) Next(result interface{}) bool {
	if len(i.docs) > 0 {
		// pop it and save the value into result
		var x interface{}
		x, i.docs = i.docs[0], i.docs[1:len(i.docs)]

		rv := reflect.ValueOf(result)
		p := reflect.Indirect(rv)
		p.Set(reflect.ValueOf(x))
		return true
	}

	// Else there is nothing more
	return false
}

func (i *mongoIter) Err() error {
	return nil
}

// errIter will error on any reads and report an error every time
type errIter struct {
	err error
}

// errIter behaves as an empty query set always returning false
func (i *errIter) Next(result interface{}) bool {
	result = nil
	return false
}

func (i *errIter) Err() error {
	return i.err
}

var testData = []struct {
	GivenIter    Iter
	ExpectedRows []optimus.Row
	ExpectedErr  error
}{
	// successful read case
	{
		GivenIter: &mongoIter{
			[]interface{}{
				// Various shapes because Docs are polymorphic
				map[string]interface{}{"field1": "field1_data"},
				map[string]interface{}{"field2": "field2_data"},
				map[string]interface{}{"field3": "field3_data"},
				map[string]interface{}{"field1": "field1_data", "field2": "field2_data"},
				map[string]interface{}{"field1": "field1_data", "field3": "field3_data"},
				map[string]interface{}{"field2": "field2_data", "field3": "field3_data"},
				map[string]interface{}{"field1": "field1_data", "field2": "field2_data", "field3": "field3_data"},
			},
		},
		ExpectedRows: []optimus.Row{
			{"field1": "field1_data"},
			{"field2": "field2_data"},
			{"field3": "field3_data"},
			{"field1": "field1_data", "field2": "field2_data"},
			{"field1": "field1_data", "field3": "field3_data"},
			{"field2": "field2_data", "field3": "field3_data"},
			{"field1": "field1_data", "field2": "field2_data", "field3": "field3_data"},
		},
	},
	// Error read case
	{
		&errIter{
			fmt.Errorf("Intentional Testing Error"),
		},
		[]optimus.Row{},
		errors.New("Intentional Testing Error"),
	},
}

// We should be able to get our rows/errors from a good and error iterator
func TestSuccessRows(t *testing.T) {
	t.Parallel()
	for _, data := range testData {
		// Build the optimusTable
		sourceTable := New(data.GivenIter)

		// Assert the result rows and errors occur
		assert.Equal(t, data.ExpectedRows, tests.GetRows(sourceTable))
		assert.Equal(t, data.ExpectedErr, sourceTable.Err())
	}
}
