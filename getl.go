package getl

// Table is a representation of a table of data.
type Table interface {
	// Rows returns a channel that provides the rows.
	Rows() chan Row
	// Err returns the first non-EOF error that was encountered by the Table.
	Err() error
}

// Row is a representation of a line of data in a Table.
type Row map[string]interface{}
