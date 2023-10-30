package transforms

import (
	"encoding/json"
	"sort"

	"github.com/Clever/optimus/v4"
)

type rows struct {
	less func(optimus.Row, optimus.Row) (bool, error)
	rows []optimus.Row
	err  error
}

func (r *rows) Len() int {
	return len(r.rows)
}
func (r *rows) Less(i, j int) bool {
	less, err := r.less(r.rows[i], r.rows[j])
	if err != nil {
		r.err = err
	}
	return less
}
func (r *rows) Swap(i, j int) {
	r.rows[i], r.rows[j] = r.rows[j], r.rows[i]
}

func sorter(sorter func(sort.Interface), less func(optimus.Row, optimus.Row) (bool, error)) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		rows := &rows{rows: []optimus.Row{}, less: less}
		for row := range in {
			rows.rows = append(rows.rows, row)
		}
		sorter(rows)
		if rows.err != nil {
			return rows.err
		}
		for _, row := range rows.rows {
			out <- row
		}
		return nil
	}
}

// Sort takes in a function that reports whether the row i should sort before row j.
// It outputs the rows in sorted order. The sort is not guaranteed to be stable.
func Sort(less func(i, j optimus.Row) (bool, error)) optimus.TransformFunc {
	return sorter(sort.Sort, less)
}

// StableSort takes in a function that reports whether the row i should sort before row j.
// It outputs the rows in stably sorted order.
func StableSort(less func(i, j optimus.Row) (bool, error)) optimus.TransformFunc {
	return sorter(sort.Stable, less)
}

type compressedRow struct {
	key  interface{}
	blob []byte
}

type compressedSorter struct {
	rows []compressedRow
	err  error
}

func (r *compressedSorter) Len() int { return len(r.rows) }

// NOTE: we cannot test this panic because Optimus transforms operate in a seperate goroutine
// that is not propagated back up to the main goroutine.
//
// > A panic cannot be recovered by a different goroutine.
// https://github.com/golang/go/wiki/PanicAndRecover
func (r *compressedSorter) Less(i, j int) bool {
	switch key1 := r.rows[i].key.(type) {
	case int:
		return key1 < r.rows[j].key.(int)
	case float64:
		return key1 < r.rows[j].key.(float64)
	case string:
		return key1 < r.rows[j].key.(string)
	default:
		panic("RowIdentifier functions for StableCompressedSort must return an int/float64/string.")
	}
}

func (r *compressedSorter) Swap(i, j int) { r.rows[i], r.rows[j] = r.rows[j], r.rows[i] }

func compressedSort(sorter func(sort.Interface), getKeyVal RowIdentifier) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		rows := &compressedSorter{rows: []compressedRow{}}
		for row := range in {
			// obtain the key for the row and retain the key + blob while sorting
			keyVal, err := getKeyVal(row)
			if err != nil {
				return err
			}

			rowBlob, err := json.Marshal(row)
			if err != nil {
				return err
			}

			rows.rows = append(rows.rows, compressedRow{
				key:  keyVal,
				blob: rowBlob,
			})
		}

		sorter(rows)

		if rows.err != nil {
			return rows.err
		}
		for _, row := range rows.rows {
			var r optimus.Row
			if err := json.Unmarshal(row.blob, &r); err != nil {
				return err
			}
			out <- r
		}
		return nil
	}
}

// StableCompressedSort sorts an Optimus table based on the provided RowIdentifier. If the
// RowIdentifier returns values that are not an int, float64 or string, the function will panic.
// It outputs the rows in stably sorted order.
func StableCompressedSort(getKey RowIdentifier) optimus.TransformFunc {
	return compressedSort(sort.Stable, getKey)
}
