# combiner
--
    import "github.com/azylman/optimus/combiner"


## Usage

#### type Combiner

```go
type Combiner struct {
}
```

A Combiner allows you to join two tables

#### func  New

```go
func New(leftTable optimus.Table, rightTable optimus.Table) *Combiner
```
New returns a Combiner that allows you to join tables.

#### func (Combiner) Extend

```go
func (c Combiner) Extend() optimus.Table
```
Extend combines two tables of length x and y into one table of length (x+y)

#### func (Combiner) Join

```go
func (c Combiner) Join(leftHeader string, rightHeader string) optimus.Table
```
Join takes two tables and connects them based on shared column values.
