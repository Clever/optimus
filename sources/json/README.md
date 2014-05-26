# json
--
    import "github.com/azylman/getl/sources/json"


## Usage

#### func  New

```go
func New(filename string) getl.Table
```
New returns a new Table that scans over the rows of a file of newline-separate
JSON objects.
