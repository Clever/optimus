# mongo
--
    import "gopkg.in/Clever/optimus.v3/sources/mongo"


## Usage

#### func  New

```go
func New(iter Iter) optimus.Table
```
New returns a new Table that iterates over all the results of a mongo query.

#### type Iter

```go
type Iter interface {
	Next(result interface{}) bool
	Err() error
}
```

Iter simulates the gopkg.in/mgo.v2.Iter interface so we can remain independent
