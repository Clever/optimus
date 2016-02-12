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

ErrorTable implemements an Optimus Table It's purpose is to return a given error

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
Err returns an ErrorTable's Error

#### func (*ErrorTable) Rows

```go
func (e *ErrorTable) Rows() <-chan optimus.Row
```
Rows returns the chan for an ErrorTable's Rows note this should only return an
error

#### func (*ErrorTable) Stop

```go
func (e *ErrorTable) Stop()
```
Stop fulfills the requirement for ErrorTable to implement the Stop function of
an Optimus Table
