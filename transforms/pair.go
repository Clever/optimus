package transforms

import (
	"github.com/facebookgo/errgroup"
	"gopkg.in/Clever/optimus.v3"
)

// RowIdentifier takes in a row and returns something that uniquely identifies the Row.
type RowIdentifier func(optimus.Row) (interface{}, error)

// KeyIdentifier is a convenience function that returns a RowIdentifier that identifies the row
// based on the value of a key in the Row.
func KeyIdentifier(key string) RowIdentifier {
	return func(row optimus.Row) (interface{}, error) {
		return row[key], nil
	}
}

var (
	// LeftJoin keeps any row where a Row was found in the left Table.
	LeftJoin = mustHave("left")
	// RightJoin keeps any row where a Row was found in the right Table.
	RightJoin = mustHave("right")
	// InnerJoin keeps any row where a Row was found in both Tables.
	InnerJoin = mustHave("left", "right")
	// OuterJoin keeps all rows.
	OuterJoin = mustHave()
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
// on the given identifier functions and join type.
func Pair(rightTable optimus.Table, leftID, rightID RowIdentifier, filterFn func(optimus.Row) (bool, error)) optimus.TransformFunc {
	// Map of everything in the right table
	right := make(map[interface{}][]optimus.Row)
	// Track whether or not rows in the right table were joined against
	joined := make(map[interface{}]bool)

	// Start building the map right away, because it could be slow.
	mapResult := make(chan error)
	go func() {
		defer close(mapResult)
		for row := range rightTable.Rows() {
			id, err := rightID(row)
			if err != nil {
				mapResult <- err
				return
			}
			if val := right[id]; val == nil {
				right[id] = []optimus.Row{}
				joined[id] = false
			}
			right[id] = append(right[id], row)
		}
		mapResult <- rightTable.Err()
	}()

	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		if err := <-mapResult; err != nil {
			return err
		}
		// The channel of paired rows from the left and right tables
		pairedRows := make(chan optimus.Row)

		wg := errgroup.Group{}
		// Pair the left table with the right table based on the ids
		wg.Add(1)
		go func() {
			defer close(pairedRows)
			defer wg.Done()

			for leftRow := range in {
				id, err := leftID(leftRow)
				if err != nil {
					wg.Error(err)
					return
				}
				if rightRows := right[id]; rightRows != nil && id != nil {
					joined[id] = true
					for _, rightRow := range rightRows {
						pairedRows <- optimus.Row{"left": leftRow, "right": rightRow}
					}
				} else {
					pairedRows <- optimus.Row{"left": leftRow}
				}
			}

			for id, joined := range joined {
				if joined {
					continue
				}
				for _, rightRow := range right[id] {
					pairedRows <- optimus.Row{"right": rightRow}
				}
			}
			return
		}()

		// Filter the paired rows based on our join type
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := Select(filterFn)(pairedRows, out); err != nil {
				wg.Error(err)
			}
		}()
		return wg.Wait()
	}
}
