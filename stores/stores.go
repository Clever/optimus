package stores

import (
	"gopkg.in/Clever/optimus.v3"
)

// Store augments the Table interface by allowing you to insert rows
type Store interface {
	optimus.Table
	AddRow(row optimus.Row) error
}

// GroupedStore augments the Table interface by allowing you to insert
// rows into groups (rows of rows)
// { "_id": "groupKey", "values":[row, row, row]}
type GroupedStore interface {
	optimus.Table
	AddRowToGroup(row optimus.Row, groupKey interface{}) error
}
