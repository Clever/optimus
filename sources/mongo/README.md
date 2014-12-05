# mongo
--
    import "gopkg.in/Clever/optimus.v3/sources/mongo"


## Usage

#### func  New

```go
func New(q *mgo.Query) optimus.Table
```
New returns a new Table that iterates over all the results of a mongo query.
