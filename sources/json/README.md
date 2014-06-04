# json
--
    import "github.com/azylman/optimus/sources/json"


## Usage

#### func  New

```go
func New(filename string) optimus.Table
```
New returns a new Table that scans over the rows of a file of newline-separate
JSON objects.
