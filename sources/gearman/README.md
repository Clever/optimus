# gearman
--
    import "gopkg.in/Clever/optimus.v3/sources/gearman"


## Usage

#### func  New

```go
func New(client gearman.Client, fn string, workload []byte,
	converter func([]byte) (optimus.Row, error)) optimus.Table
```
New returns a new Table that outputs the worker data from a Gearman job.
Converter should be a function that knows how to take a data event from Gearman
and turn it into a Row.
