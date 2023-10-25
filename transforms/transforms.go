package transforms

import (
	"fmt"
	"sync"

	"github.com/Clever/optimus/v4"
	"gopkg.in/fatih/set.v0"
)

// TableTransform returns a TransformFunc that applies the given transform function.
func TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		for row := range in {
			if err := transform(row, out); err != nil {
				return err
			}
		}
		return nil
	}
}

// Select returns a TransformFunc that removes any rows that don't pass the filter.
func Select(filter func(optimus.Row) (bool, error)) optimus.TransformFunc {
	return TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
		pass, err := filter(row)
		if err != nil || !pass {
			return err
		}
		out <- row
		return nil
	})
}

// Map returns a TransformFunc that transforms every row with the given function.
func Map(transform func(optimus.Row) (optimus.Row, error)) optimus.TransformFunc {
	return TableTransform(func(in optimus.Row, out chan<- optimus.Row) error {
		row, err := transform(in)
		if err != nil {
			return err
		}
		out <- row
		return nil
	})
}

// Each returns a TransformFunc that makes no changes to the table, but calls the given function
// on every Row.
func Each(fn func(optimus.Row) error) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		if err := fn(row); err != nil {
			return nil, err
		}
		return row, nil
	})
}

// Fieldmap returns a TransformFunc that applies a field mapping to every Row.
func Fieldmap(mappings map[string][]string) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		newRow := optimus.Row{}
		for key, vals := range mappings {
			for _, val := range vals {
				if oldRowVal, ok := row[key]; ok {
					newRow[val] = oldRowVal
				}
			}
		}
		return newRow, nil
	})
}

// SafeFieldmap returns a TransformFunc that applies a field mapping to every Row.
// Exactly like Fieldmap except this one will error for multiple mappings to the same value.
func SafeFieldmap(mappings map[string][]string) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		newRow := optimus.Row{}
		for key, vals := range mappings {
			for _, val := range vals {
				if oldRowVal, ok := row[key]; ok {
					if _, ok := newRow[val]; ok {
						return nil, fmt.Errorf("Detected multiple mappings to the same value for key %s", val)
					}
					newRow[val] = oldRowVal
				}
			}
		}
		return newRow, nil
	})
}

// Valuemap returns a TransformFunc that applies a value mapping to every Row.
func Valuemap(mappings map[string]map[interface{}]interface{}) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		newRow := optimus.Row{}
		for key, val := range row {
			if mappings[key] == nil || mappings[key][val] == nil {
				newRow[key] = val
				continue
			}
			newRow[key] = mappings[key][val]
		}
		return newRow, nil
	})
}

type joinStruct struct {
	Left, Inner joinType
}

type joinType struct {
	int
}

// JoinType describes the type of join.
// Left: Always add row from Left table, even if no corresponding rows found in Right table)
// Inner: Only add row from Left table if corresponding row(s) found in Right table)
var JoinType = joinStruct{Left: joinType{0}, Inner: joinType{1}}

// Join returns a TransformFunc that joins Rows with another table using the specified join type.
func Join(rightTable optimus.Table, leftHeader string, rightHeader string, join joinType) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		var filterFn func(optimus.Row) (bool, error)
		switch join {
		case JoinType.Left:
			filterFn = LeftJoin
		case JoinType.Inner:
			filterFn = InnerJoin
		}

		unmergedOut := make(chan optimus.Row)
		pairer := Pair(rightTable, KeyIdentifier(leftHeader), KeyIdentifier(rightHeader), filterFn)

		errs := make(chan error, 1)

		go func() {
			defer close(unmergedOut)
			defer close(errs)
			errs <- pairer(in, unmergedOut)
		}()
		for row := range unmergedOut {
			out <- mergePairs(row)
		}
		return <-errs
	}
}

func mergePairs(pairs optimus.Row) optimus.Row {
	// Assume left exists because that's how Join uses it
	left := pairs["left"].(optimus.Row)
	right := pairs["right"]
	if right == nil {
		return left
	}
	output := optimus.Row{}
	for k, v := range left {
		output[k] = v
	}
	for k, v := range right.(optimus.Row) {
		output[k] = v
	}
	return output
}

// Reduce returns a TransformFunc that reduces all the Rows to a single Row.
func Reduce(fn func(accum, item optimus.Row) error) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		accum := optimus.Row{}
		for row := range in {
			if err := fn(accum, row); err != nil {
				return err
			}
		}
		out <- accum
		return nil
	}
}

// Concurrently returns a TransformFunc that applies the given TransformFunc a number of times
// concurrently, based on the supplied concurrency count.
func Concurrently(fn optimus.TransformFunc, concurrency int) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		wg := sync.WaitGroup{}
		wg.Add(concurrency)
		errs := make(chan error)
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				if err := fn(in, out); err != nil {
					errs <- err
				}
			}()
		}
		go func() {
			wg.Wait()
			close(errs)
		}()
		for err := range errs {
			return err
		}
		return nil
	}
}

// Concat returns a TransformFunc that concatenates all the Rows in the input Tables, in order.
func Concat(tables ...optimus.Table) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		for row := range in {
			out <- row
		}
		for _, table := range tables {
			for row := range table.Rows() {
				out <- row
			}
			if table.Err() != nil {
				return table.Err()
			}
		}
		return nil
	}
}

// Unique returns a TransformFunc that returns Rows that are unique, according to the specified hash.
// No order is guaranteed for the unique row which is returned.
func Unique(hash RowIdentifier) optimus.TransformFunc {
	set := set.New()
	return Select(func(row optimus.Row) (bool, error) {
		hashedRow, err := hash(row)
		if err != nil {
			return false, err
		}
		if !set.Has(hashedRow) {
			set.Add(hashedRow)
			return true, nil
		}
		return false, nil
	})
}

// GroupBy returns a TransformFunc that returns Rows of Rows grouped by their identifier.
// The identifier must be comparable.
// Each output row is one group of rows. The output row has two fields: id, which is the identifier
// for that group, and rows, which is the slice of Rows that share that identifier. For example,
// one output row in a grouping by the "group" field might look like:
// optimus.Row{"id": "a", "rows": []optimus.Row{{"group": "a", "val": 2"}, {"group": "a", "val": 3}}}
func GroupBy(identifier RowIdentifier) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		groups := map[interface{}][]optimus.Row{}
		for row := range in {
			val, err := identifier(row)
			if err != nil {
				return err
			}
			if groups[val] == nil {
				groups[val] = []optimus.Row{}
			}
			groups[val] = append(groups[val], row)
		}
		for id, rows := range groups {
			out <- optimus.Row{"id": id, "rows": rows}
		}
		return nil
	}
}

// RowFilter is meant to return `true` if a section is meant to be filtered out.
type RowFilter func(optimus.Row) bool

// passthroughTable is a super simple optimus table that just passes rows through
type passthroughTable chan optimus.Row

func (p passthroughTable) Rows() <-chan optimus.Row {
	return p
}
func (p passthroughTable) Err() error {
	return nil
}
func (p passthroughTable) Stop() {}

// BypassTransforms effectively wraps a slice of transform funcs with a gate to conditionally
// apply the transforms only if they match the filter.
func BypassTransforms(doBypass RowFilter, optionalTransforms []optimus.TransformFunc) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		// setup an optimus pipeline to process rows through all the provided optional transforms
		passthroughChan := make(chan optimus.Row, 0)
		tempTable := optimus.Table(passthroughTable(passthroughChan))
		for _, t := range optionalTransforms {
			tempTable = optimus.Transform(tempTable, t)
		}

		// we do not have any guarantees about how many rows will be outputted so we
		// pass all the rows through until we have consumed all of them.
		doneChan := make(chan bool)
		go func() {
			for sunkRow := range tempTable.Rows() {
				out <- sunkRow
			}
			doneChan <- true
		}()

		for row := range in {
			if doBypass(row) {
				out <- row
				continue
			}
			passthroughChan <- row
		}

		// we've consumed all the input rows and passed them through so now we can close the
		// passthroughChan to trigger the bypass transforms to stop executing
		close(passthroughChan)

		// wait for all the rows from the bypass transforms to be consumed
		<-doneChan
		return nil
	}
}
