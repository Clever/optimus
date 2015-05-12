# error
--
    import "gopkg.in/Clever/optimus.v3/sources/error"


## Usage

#### type ErrorTable

```go
type ErrorTable struct {
	Stopped bool
}
```


#### func  New

```go
func New(err error) *ErrorTable
```
New returns a new Table that returns a given error. Primarily used for testing
purposes.

#### func (*ErrorTable) Err

```go
func (e *ErrorTable) Err() error
```

#### func (*ErrorTable) Rows

```go
func (e *ErrorTable) Rows() <-chan optimus.Row
```

#### func (*ErrorTable) Stop

```go
func (e *ErrorTable) Stop()
```
