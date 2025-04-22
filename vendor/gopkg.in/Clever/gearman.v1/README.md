# gearman
--
    import "gopkg.in/Clever/gearman.v1"

Package gearman provides a thread-safe Gearman client


### Example

Here's an example program that submits a job to Gearman and listens for events
from that job:

    package main

    import(
    	"gopkg.in/Clever/gearman.v1"
    	"gopkg.in/Clever/gearman.v1/job"
    	"ioutil"
    )

    func main() {
    	client, err := gearman.NewClient("tcp4", "localhost:4730")
    	if err != nil {
    		panic(err)
    	}

    	j, err := client.Submit("reverse", []byte("hello world!"), nil, nil)
    	if err != nil {
    		panic(err)
    	}
    	state := j.Run()
    	println(state) // job.Completed
    	data, err := ioutil.ReadAll(j.Data())
    	if err != nil {
    		panic(err)
    	}
    	println(data) // !dlrow olleh
    }

## Usage

#### type Client

```go
type Client interface {
	// Closes the connection to the server
	Close() error
	// Submits a new job to the server with the specified function and payload. You must provide two
	// WriteClosers for data and warnings to be written to.
	Submit(fn string, payload []byte, data, warnings io.WriteCloser) (job.Job, error)
}
```

Client is a Gearman client

#### func  NewClient

```go
func NewClient(network, addr string) (Client, error)
```
NewClient returns a new Gearman client pointing at the specified server
