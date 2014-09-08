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
		"os"
	)

	func main() {
		f, err := os.Open("example1.csv")
		out, err := os.Create("output.csv")
		defer out.Close()
		begin := csvSource.New(f)
		step1 := optimus.Transform(begin, transforms.Fieldmap(fieldMappings))
		step2 := optimus.Transform(step1, transforms.Valuemap(valueMappings))
		end := optimus.Transform(step2, transforms.Map(arbitraryTransformFunction))
		err := csvSink.New(out)(end)
	}

Here's one that uses chaining:

	package optimus

	import(
		csvSource "gopkg.in/azylman/optimus.v1/sources/csv"
		csvSink "gopkg.in/azylman/optimus.v1/sinks/csv"
		"gopkg.in/azylman/optimus.v1"
		"gopkg.in/azylman/optimus.v1/transformer"
		"os"
	)

	func main() {
		f, err := os.Open("example1.csv")
		out, err := os.Create("output.csv")
		defer out.Close()
		begin := csvSource.New(f)
		err := transformer.New(begin).Fieldmap(fieldMappings).Valuemap(
			valueMappings).Map(arbitraryTransformFunction).Sink(csvSink.New(out))
	}

*/
package optimus
