package transforms

import (
	"gopkg.in/Clever/optimus.v3"
)

// PairType is the type of join to use when Pairing
type PairType int

// RowHasher takes in a row and returns a hash for that Row.
// Used when Pairing.
type RowHasher func(optimus.Row) interface{}

// KeyHasher is a convenience function that returns a RowHasher that hashes based on the value of a
// key in the Row.
func KeyHasher(key string) RowHasher {
	return func(row optimus.Row) interface{} {
		return row[key]
	}
}

const (
	// LeftJoin keeps any row where a Row was found in the left Table.
	LeftJoin PairType = iota
	// RightJoin keeps any row where a Row was found in the right Table.
	RightJoin
	// InnerJoin keeps any row where a Row was found in both Tables.
	InnerJoin
	// OuterJoin keeps all rows.
	OuterJoin
)

// Pair returns a TransformFunc that pairs all the elements in the table with another table, based
// on the given hashing functions and join type.
func Pair(rightTable optimus.Table, leftHash, rightHash RowHasher, join PairType) optimus.TransformFunc {
	right := make(map[interface{}][]optimus.Row)
	found := make(map[interface{}]bool)

	// Build hash from right table
	doneRight := make(chan struct{})
	go func() {
		defer close(doneRight)
		for row := range rightTable.Rows() {
			hash := rightHash(row)
			// Initialize if dne
			if val := right[hash]; val == nil {
				right[hash] = []optimus.Row{}
				found[hash] = false
			}
			right[hash] = append(right[hash], row)
		}
	}()

	// Function that pairs everything in the in channel with the right table. Outputs rows in the
	// form {"left": leftRow, "right": rightRow}.
	// Sends everything, with no concern for join type - that's handled later.
	pair := func(in <-chan optimus.Row, out chan<- optimus.Row) {
		defer close(out)
		// Wait until we're done building right hash
		<-doneRight
		if rightTable.Err() != nil {
			return
		}

		for leftRow := range in {
			hash := leftHash(leftRow)
			if rightRows := right[hash]; rightRows != nil && hash != nil {
				found[hash] = true
				for _, rightRow := range rightRows {
					out <- optimus.Row{"left": leftRow, "right": rightRow}
				}
			} else {
				out <- optimus.Row{"left": leftRow}
			}
		}

		for hash, found := range found {
			if found {
				continue
			}
			for _, rightRow := range right[hash] {
				out <- optimus.Row{"right": rightRow}
			}
		}
		return
	}

	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		unfilteredOut := make(chan optimus.Row)
		go pair(in, unfilteredOut)
		// Filtered the paired rows based on join type.
		for row := range unfilteredOut {
			switch join {
			case OuterJoin:
				out <- row
			case InnerJoin:
				if row["right"] != nil && row["left"] != nil {
					out <- row
				}
			case LeftJoin:
				if row["left"] != nil {
					out <- row
				}
			case RightJoin:
				if row["right"] != nil {
					out <- row
				}
			}
		}
		return rightTable.Err()
	}

}
