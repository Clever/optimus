/*
Package getl provides methods for manipulating tables of data.

Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package getl

	import(
		"github.com/azylman/getl"
		csvSource "github.com/azylman/getl/sources/csv"
		csvSink "github.com/azylman/getl/sinks/csv"
		"github.com/azylman/getl/transforms"
	)

	func main() {
		begin := csv.Source("example1.csv")
		step1 := getl.Transform(begin, transforms.Fieldmap(fieldMappings))
		step2 := getl.Transform(step1, transforms.Valuemap(valueMappings))
		end := getl.Transform(step2, transforms.Map(arbitraryTransformFunction))
		err := csv.Sink(end, "output.csv")
	}

Here's one that uses chaining:

	package getl

	import(
		"github.com/azylman/getl"
		"github.com/azylman/getl/sources/csv"
		"github.com/azylman/getl/transformer"
	)

	func main() {
		begin := csvSource.Source("example1.csv")
		end := transformer.New(begin).Fieldmap(fieldMappings).Valuemap(
			valueMappings).Map(arbitraryTransformFunction).Table()
		err := csvSink.Sink(end, "output.csv")
	}

*/
package getl
