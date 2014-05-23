package getl

// Table is a representation of a table of data.
type Table interface {
	// Rows returns a channel that provides the rows.
	Rows() <-chan Row
	// Err returns the first non-EOF error that was encountered by the Table.
	Err() error
	// Stop signifies that a Table should stop sending Rows down its channel.
	// A Table is also responsible for calling Stop on any upstream Tables it knows about.
	// Stop should be idempotent. It's expected that Stop will never be called by a consumer of a
	// Table, unless that consumer is also a Table. It can be used to Stop all upstream Tables in
	// the event of an error that needs to halt the pipeline.
	Stop()
}

// Row is a representation of a line of data in a Table.
type Row map[string]interface{}

// TransformFunc is a function that can be applied to a Table to transform it.
type TransformFunc func(<-chan Row, chan<- Row) error

// Transform returns a new Table that provides all the Rows of the input Table transformed with the TransformFunc.
func Transform(source Table, transform TransformFunc) Table {
	// TODO
	return source
}
