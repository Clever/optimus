# json
--
    import "github.com/Clever/optimus/v4/sinks/json"


## Usage

#### func  New

```go
func New(out io.Writer) optimus.Sink
```
New writes all of the Rows in a Table as newline-separate JSON objects.
