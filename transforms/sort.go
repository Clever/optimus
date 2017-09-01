package transforms

import (
	"encoding/json"
	"fmt"
	"sort"

	"gopkg.in/Clever/optimus.v3"
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
	key  string
	blob []byte
}

type compressedSorter struct {
	rows []compressedRow
	err  error
}

func (r *compressedSorter) Len() int           { return len(r.rows) }
func (r *compressedSorter) Less(i, j int) bool { return r.rows[i].key < r.rows[j].key }
func (r *compressedSorter) Swap(i, j int)      { r.rows[i], r.rows[j] = r.rows[j], r.rows[i] }

func compressedSort(sorter func(sort.Interface), getKey func(optimus.Row) (string, error)) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		rows := &compressedSorter{rows: []compressedRow{}}
		for row := range in {
			// obtain the key for the row and retain the key + blob while sorting
			key, err := getKey(row)
			if err != nil {
				return err
			}

			rowBlob, err := json.Marshal(row)
			if err != nil {
				return err
			}

			rows.rows = append(rows.rows, compressedRow{
				key:  key,
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

// StableCompressedSort takes in a function that reports whether the row i should sort before row j.
// It outputs the rows in stably sorted order.
func StableCompressedSort(getKey func(optimus.Row) (string, error)) optimus.TransformFunc {
	return compressedSort(sort.Stable, getKey)
}

// GetKey is utility function for Optimus' StableCompressedSort functionality.
func GetKey(key string) func(optimus.Row) (string, error) {
	return func(r optimus.Row) (string, error) {
		// Don't crash on empty objects
		if r[key] == nil {
			return "", nil
		}
		k, ok := r[key].(string)
		if !ok {
			return "", fmt.Errorf("%s wasn't a string, had value: %#v", key, r[key])
		}
		return k, nil
	}
}
