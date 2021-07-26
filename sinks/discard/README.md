# discard
--
    import "github.com/Clever/optimus/v4/sinks/discard"


## Usage

```go
var Discard = func(t optimus.Table) error {
	for _ = range t.Rows() {
	}
	return t.Err()
}
```
Discard is a Sink that discards all the Rows in the Table and returns any error.
