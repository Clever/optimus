# job
--
    import "gopkg.in/Clever/gearman.v1/job"


## Usage

#### type Job

```go
type Job interface {
	// The handle of the job
	Handle() string
	// Status returns the current status of the gearman job
	Status() Status
	// Blocks until the job completes. Returns the state, Completed or Failed.
	Run() State
}
```

Job represents a Gearman job

#### func  New

```go
func New(handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) Job
```
New creates a new Gearman job with the specified handle, updating the job based
on the packets in the packets channel. The only packets coming down packets
should be packets for this job. It also takes in two WriteClosers to right job
data and warnings to.

#### type State

```go
type State int
```

State of a Gearman job

```go
const (
	// Running means that the job has not yet finished
	Running State = iota
	// Completed means that the job finished successfully
	Completed
	// Failed means that the job failed
	Failed
)
```

#### type Status

```go
type Status struct {
	Numerator   int
	Denominator int
}
```

Status of a Gearman job
