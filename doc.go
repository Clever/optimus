/*
Package optimus provides methods for manipulating tables of data.

Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package optimus

	import(
		csvSource "gopkg.in/azylman/optimus.v1/sources/csv"
		csvSink "gopkg.in/azylman/optimus.v1/sinks/csv"
		"gopkg.in/azylman/optimus.v1"
		"gopkg.in/azylman/optimus.v1/transforms"
	)

	func main() {
		begin := csvSource.New("example1.csv")
		step1 := optimus.Transform(begin, transforms.Fieldmap(fieldMappings))
		step2 := optimus.Transform(step1, transforms.Valuemap(valueMappings))
		end := optimus.Transform(step2, transforms.Map(arbitraryTransformFunction))
		err := csvSink.New("output.csv")(end)
	}

Here's one that uses chaining:

	package optimus

	import(
		csvSource "gopkg.in/azylman/optimus.v1/sources/csv"
		csvSink "gopkg.in/azylman/optimus.v1/sinks/csv"
		"gopkg.in/azylman/optimus.v1"
		"gopkg.in/azylman/optimus.v1/transformer"
	)

	func main() {
		begin := csvSource.New("example1.csv")
		err := transformer.New(begin).Fieldmap(fieldMappings).Valuemap(
			valueMappings).Map(arbitraryTransformFunction).Sink(csvSink.New("output.csv"))
	}

*/
package optimus
