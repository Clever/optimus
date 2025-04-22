/*
Package gearman provides a thread-safe Gearman client

Example

Here's an example program that submits a job to Gearman and listens for events from that job:

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
*/
package gearman
