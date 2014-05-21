package getl

// Table is a representation of a table of data.
type Table interface {
	// Rows returns a channel that provides the rows.
	Rows() chan Row
	// Err returns the first non-EOF error that was encountered by the Table.
	Err() error
	// Stop signifies that a Table should stop sending Rows down its channel.
	// A Table is also responsible for calling Stop on any upstream Tables it knows about.
	// Stop should be idempotent.
	Stop()
}

// Row is a representation of a line of data in a Table.
type Row map[string]interface{}
