# csv
--
    import "github.com/azylman/optimus/sinks/json"


## Usage

#### func  New

```go
func New(source optimus.Table, filename string) error
```
New writes all of the Rows in a Table as newline-separate JSON objects.
