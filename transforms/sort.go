package transforms

import (
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
