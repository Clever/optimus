/*
Package getl provides methods for manipulating tables of data.

Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package getl

	import(
		"github.com/azylman/getl/table/csv"
		"github.com/azylman/getl/transform"
	)

	func main() {
		begin := csv.NewTable("example1.csv")
		step1 := transform.Fieldmap(begin, fieldMappings)
		end := transform.Valuemap(step1, valueMappings)
		err := csv.FromTable(end, "output.csv")
	}

Here's one that uses chaining:

	package getl

	import(
		"github.com/azylman/getl/table/csv"
		"github.com/azylman/getl/transform"
	)

	func main() {
		begin := csv.NewTable("example1.csv")
		end, err := transform.NewTransformer(begin)
			.Fieldmap(fieldMappings)
			.Valuemap(valueMappings)
			.Table()
		err := csv.FromTable(end, "output.csv")
	}

*/
package getl
