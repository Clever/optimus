# scanner
--
    import "gopkg.in/Clever/gearman.v1/scanner"


## Usage

#### func  New

```go
func New(r io.Reader) *bufio.Scanner
```
New returns a new Scanner that parses a Reader as the Gearman protocol. See:
http://gearman.org/protocol/
