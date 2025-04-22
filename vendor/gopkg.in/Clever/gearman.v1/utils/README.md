# utils
--
    import "gopkg.in/Clever/gearman.v1/utils"


## Usage

#### type Buffer

```go
type Buffer struct {
	bytes.Buffer
}
```

Buffer is a WriteCloser that wraps a bytes.Buffer

#### func  NewBuffer

```go
func NewBuffer() *Buffer
```
NewBuffer initializes an empty Buffer

#### func (Buffer) Close

```go
func (b Buffer) Close() error
```
Close noop
