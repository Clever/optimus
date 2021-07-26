package discard

import (
	"github.com/Clever/optimus/v4"
)

// Discard is a Sink that discards all the Rows in the Table and returns any error.
var Discard = func(t optimus.Table) error {
	for range t.Rows() {
	}
	return t.Err()
}
