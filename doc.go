/*
Package optimus provides interfaces and methods for lazily, concurrently manipulating collections of data.

The Table interfaces is at the core of optimus. A Table is a lazy collection of data.
Several implementations of Tables are provided for extracting data ("sources"), such as from a CSV or Mongo.

A TransformFunc is a function that can be applied to a Table to lazily, concurrently modify the data.
Several implementations of TransformFuncs are provided for common transformations (e.g. Map).

Lastly, a set of Sink functions are provided that will "sink" a table into some output, such as a CSV.


Example

Here's an example program that performs a set of field and value mappings on a CSV file:

	package optimus

	import(
		csvSource "gopkg.in/Clever/optimus.v3/sources/csv"
		csvSink "gopkg.in/Clever/optimus.v3/sinks/csv"
		"gopkg.in/Clever/optimus.v3"
		"gopkg.in/Clever/optimus.v3/transforms"
		"os"
	)

	func main() {
		// Errors ignored for brevity
		f, _ := os.Open("example1.csv")
		out, _ := os.Create("output.csv")
		defer out.Close()
		begin := csvSource.New(f)
		step1 := optimus.Transform(begin, transforms.Fieldmap(fieldMappings))
		step2 := optimus.Transform(step1, transforms.Valuemap(valueMappings))
		end := optimus.Transform(step2, transforms.Map(arbitraryTransformFunction))
		csvSink.New(out)(end)
	}

Here's one that uses chaining:

	package optimus

	import(
		csvSource "gopkg.in/Clever/optimus.v3/sources/csv"
		csvSink "gopkg.in/Clever/optimus.v3/sinks/csv"
		"gopkg.in/Clever/optimus.v3"
		"gopkg.in/Clever/optimus.v3/transformer"
		"os"
	)

	func main() {
		// Errors ignored for brevity
		f, _ := os.Open("example1.csv")
		out, _ := os.Create("output.csv")
		defer out.Close()
		transformer.New(csvSource.New(f)).Fieldmap(fieldMappings).Valuemap(
			valueMappings).Map(arbitraryTransformFunction).Sink(csvSink.New(out))
	}

*/
package optimus
