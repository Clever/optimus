# csv
--
    import "gopkg.in/Clever/optimus.v3/sources/csv"


## Usage

#### func  New

```go
func New(in io.Reader) optimus.Table
```
New returns a new Table that scans over the rows of a CSV.

#### func  NewWithDelimiter

```go
func NewWithDelimiter(in io.Reader, delimiter rune) optimus.Table
```
NewWithDelimiter returns a new Table that scans over the rows of a CSV delimited
as specified.
