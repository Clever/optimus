package job

import (
	"fmt"
	"gopkg.in/Clever/gearman.v1/packet"
	"io"
	"strconv"
)

// State of a Gearman job
type State int

const (
	// Running means that the job has not yet finished
	Running State = iota
	// Completed means that the job finished successfully
	Completed
	// Failed means that the job failed
	Failed
)

// Status of a Gearman job
type Status struct {
	Numerator   int
	Denominator int
}

// Job represents a Gearman job
type Job interface {
	// The handle of the job
	Handle() string
	// Status returns the current status of the gearman job
	Status() Status
	// Blocks until the job completes. Returns the state, Completed or Failed.
	Run() State
}

type job struct {
	handle         string
	data, warnings io.WriteCloser
	status         Status
	state          State
	done           chan struct{}
}

func (j job) Handle() string {
	return j.handle
}

func (j job) Status() Status {
	return j.status
}

func (j *job) Run() State {
	<-j.done
	return j.state
}

func (j *job) handlePackets(packets chan *packet.Packet) {
	for pack := range packets {
		switch pack.Type {
		case packet.WorkStatus:
			num, err := strconv.Atoi(string(pack.Arguments[1]))
			if err != nil {
				fmt.Println("Error converting numerator", err)
			}
			den, err := strconv.Atoi(string(pack.Arguments[2]))
			if err != nil {
				fmt.Println("Error converting denominator", err)
			}
			j.status = Status{Numerator: num, Denominator: den}
		case packet.WorkComplete:
			j.state = Completed
			close(j.done)
		case packet.WorkFail:
			j.state = Failed
			close(j.done)
		case packet.WorkData:
			if _, err := j.data.Write(pack.Arguments[1]); err != nil {
				fmt.Printf("Error writing data", pack.Arguments[1], err)
			}
		case packet.WorkWarning:
			if _, err := j.warnings.Write(pack.Arguments[1]); err != nil {
				fmt.Printf("Error writing warnings", pack.Arguments[1], err)
			}
		default:
			fmt.Println("WARNING: Unimplemented packet type", pack.Type)
		}
	}
}

// New creates a new Gearman job with the specified handle, updating the job based on the packets
// in the packets channel. The only packets coming down packets should be packets for this job.
// It also takes in two WriteClosers to right job data and warnings to.
func New(handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) Job {
	j := &job{
		handle:   handle,
		data:     data,
		warnings: warnings,
		status:   Status{0, 0},
		state:    Running,
		done:     make(chan struct{}),
	}
	go j.handlePackets(packets)
	return j
}
