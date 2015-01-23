package transforms

import (
	"github.com/facebookgo/errgroup"
	"gopkg.in/Clever/optimus.v3"
)

// PairType is the type of join to use when Pairing
type PairType int

// RowHasher takes in a row and returns a hash for that Row.
// Used when Pairing.
type RowHasher func(optimus.Row) (interface{}, error)

// KeyHasher is a convenience function that returns a RowHasher that hashes based on the value of a
// key in the Row.
func KeyHasher(key string) RowHasher {
	return func(row optimus.Row) (interface{}, error) {
		return row[key], nil
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

// mustHave takes in any amount of keys and returns a function that can be passed to Select
// that returns true for any Row that has all of those keys.
func mustHave(keys ...string) func(optimus.Row) (bool, error) {
	return func(row optimus.Row) (bool, error) {
		for _, key := range keys {
			if row[key] == nil {
				return false, nil
			}
		}
		return true, nil
	}
}

// Pair returns a TransformFunc that pairs all the elements in the table with another table, based
// on the given hashing functions and join type.
func Pair(rightTable optimus.Table, leftHash, rightHash RowHasher, join PairType) optimus.TransformFunc {
	// Hash of everything in the right table
	right := make(map[interface{}][]optimus.Row)
	// Track whether or not rows in the right table were joined against
	joined := make(map[interface{}]bool)

	// Start building the hash right away, because it could be slow.
	hashResult := make(chan error)
	go func() {
		defer close(hashResult)
		// Build the hash for the right table
		for row := range rightTable.Rows() {
			hash, err := rightHash(row)
			if err != nil {
				hashResult <- err
				return
			}
			if val := right[hash]; val == nil {
				right[hash] = []optimus.Row{}
				joined[hash] = false
			}
			right[hash] = append(right[hash], row)
		}
		hashResult <- rightTable.Err()
	}()

	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		if err := <-hashResult; err != nil {
			return err
		}
		// The channel of paired rows from the left and right tables
		pairedRows := make(chan optimus.Row)

		wg := errgroup.Group{}
		// Pair the left table with the right table based on the hashes
		wg.Add(1)
		go func() {
			defer close(pairedRows)
			defer wg.Done()

			for leftRow := range in {
				hash, err := leftHash(leftRow)
				if err != nil {
					wg.Error(err)
					return
				}
				if rightRows := right[hash]; rightRows != nil && hash != nil {
					joined[hash] = true
					for _, rightRow := range rightRows {
						pairedRows <- optimus.Row{"left": leftRow, "right": rightRow}
					}
				} else {
					pairedRows <- optimus.Row{"left": leftRow}
				}
			}

			for hash, joined := range joined {
				if joined {
					continue
				}
				for _, rightRow := range right[hash] {
					pairedRows <- optimus.Row{"right": rightRow}
				}
			}
			return
		}()

		// Filter the paired rows based on our join type
		wg.Add(1)
		go func() {
			defer wg.Done()
			var filter func(optimus.Row) (bool, error)
			switch join {
			case OuterJoin:
				filter = mustHave()
			case InnerJoin:
				filter = mustHave("right", "left")
			case LeftJoin:
				filter = mustHave("left")
			case RightJoin:
				filter = mustHave("right")
			}
			if err := Select(filter)(pairedRows, out); err != nil {
				wg.Error(err)
			}
		}()
		return wg.Wait()
	}
}
