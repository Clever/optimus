/*
Package getl provides methods for manipulating tables of data.

Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package getl

	import(
		"github.com/azylman/getl/sources/csv"
		"github.com/azylman/getl/transformer"
	)

	func main() {
		begin := csv.New("example1.csv")
		step1 := transformer.Fieldmap(begin, fieldMappings)
		end := transformer.RowTransform(step1, arbitraryTransformFunction)
		err := csv.FromTable(end, "output.csv")
	}

Here's one that uses chaining:

	package getl

	import(
		"github.com/azylman/getl/sources/csv"
		"github.com/azylman/getl/transformer"
	)

	func main() {
		begin := csv.New("example1.csv")
		end := transformer.New(begin)
			.Fieldmap(fieldMappings)
			.RowTransform(arbitraryTransformFunction)
			.Table()
		err := csv.FromTable(end, "output.csv")
	}

*/
package getl
