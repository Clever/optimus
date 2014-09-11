# error
--
    import "gopkg.in/Clever/optimus.v2/sources/error"


## Usage

#### func  New

```go
func New(err error) optimus.Table
```
New returns a new Table that returns a given error. Primarily used for testing
purposes.
