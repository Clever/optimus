package stores

import (
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/transforms"
)

func TableTransformToStore(transform func(optimus.Row, chan<- optimus.Row) error, store Store) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		toStore := make(chan optimus.Row)
		go func() {
			defer close(toStore)
			for row := range in {
				if err := transform(row, toStore); err != nil {
					// TODO: how to handle this error
				}
			}
		}()

		// as rows are transformed, store them
		for row := range toStore {
			store.AddRow(row)
		}
		// Once finished adding rows to the store, stream to out
		for row := range store.Rows() {
			out <- row
		}

		return nil
	}
}

func MapToStore(transform func(optimus.Row) (optimus.Row, error), store Store) optimus.TransformFunc {
	return TableTransformToStore(func(in optimus.Row, out chan<- optimus.Row) error {
		row, err := transform(in)
		if err != nil {
			return err
		}
		out <- row
		return nil
	}, store)
}

func GroupByToStore(identifier transforms.RowIdentifier, store GroupedStore) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		for row := range in {
			key, err := identifier(row)
			if err != nil {
				return err
			}

			// Store the row
			if err := store.AddRowToGroup(row, key); err != nil {
				return err
			}
		}

		for row := range store.Rows() {
			out <- row
		}
		return nil
	}
}
