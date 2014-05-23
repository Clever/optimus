/*
Package getl provides methods for manipulating tables of data.

NOTE: The API is currently very unstable.

Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package getl

	import(
		"github.com/azylman/getl"
		"github.com/azylman/getl/sources/csv"
		"github.com/azylman/getl/transforms"
	)

	func main() {
		begin := csv.NewSource("example1.csv")
		step1 := getl.Transform(begin, transforms.Fieldmap(fieldMappings))
		step2 := getl.Transform(step1, transforms.Valuemap(valueMappings))
		end := getl.Transform(step2, transforms.Row(arbitraryTransformFunction))
		err := csv.NewSink(end, "output.csv")
	}

Here's one that uses chaining:

	package getl

	import(
		"github.com/azylman/getl"
		"github.com/azylman/getl/sources/csv"
		"github.com/azylman/getl/transformer"
	)

	func main() {
		begin := csv.NewSource("example1.csv")
		end := transformer.New(begin).Fieldmap(fieldMappings).Valuemap(
			valueMappings).RowTransform(arbitraryTransformFunction).Table()
		err := csv.NewSink(end, "output.csv")
	}

*/
package getl
