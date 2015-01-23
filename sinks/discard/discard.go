package discard

import (
	"gopkg.in/Clever/optimus.v3"
)

// Discard is a Sink that discards all the Rows in the Table and returns any error.
var Discard = func(t optimus.Table) error {
	for _ = range t.Rows() {
	}
	return t.Err()
}
