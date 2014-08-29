/*
Package optimus provides methods for manipulating tables of data.

Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package optimus

	import(
		"gopkg.in/azylman/optimus.v1"
		csvSource "gopkg.in/azylman/optimus.v1/sources/csv"
		csvSink "gopkg.in/azylman/optimus.v1/sinks/csv"
		"gopkg.in/azylman/optimus.v1/transforms"
	)

	func main() {
		begin := csvSource.New("example1.csv")
		step1 := optimus.Transform(begin, transforms.Fieldmap(fieldMappings))
		step2 := optimus.Transform(step1, transforms.Valuemap(valueMappings))
		end := optimus.Transform(step2, transforms.Map(arbitraryTransformFunction))
		err := csvSink.New(end, "output.csv")
	}

Here's one that uses chaining:

	package optimus

	import(
		"gopkg.in/azylman/optimus.v1"
		csvSource "gopkg.in/azylman/optimus.v1/sources/csv"
		csvSink "gopkg.in/azylman/optimus.v1/sinks/csv"
		"gopkg.in/azylman/optimus.v1/transformer"
	)

	func main() {
		begin := csvSource.New("example1.csv")
		end := transformer.New(begin).Fieldmap(fieldMappings).Valuemap(
			valueMappings).Map(arbitraryTransformFunction).Table()
		err := csvSink.New(end, "output.csv")
	}

*/
package optimus
