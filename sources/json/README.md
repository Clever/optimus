# json
--
    import "github.com/Clever/optimus/v4/sources/json"


## Usage

#### func  New

```go
func New(in io.Reader) optimus.Table
```
New returns a new Table that scans over the rows of a file of newline-separate
JSON objects.
