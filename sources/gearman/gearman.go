package gearman

import (
	"fmt"
	"gopkg.in/Clever/gearman.v1"
	"gopkg.in/Clever/gearman.v1/job"
	gearmanUtils "gopkg.in/Clever/gearman.v1/utils"
	"gopkg.in/Clever/optimus.v3"
)

type table struct {
	rows    chan optimus.Row
	stopped bool
	err     error
}

func (t *table) Rows() <-chan optimus.Row {
	return t.rows
}

func (t table) Err() error {
	return t.err
}

func (t *table) Stop() {
	t.stopped = true
}

type getData struct {
	handler func([]byte)
}

func (rw *getData) Write(p []byte) (int, error) {
	rw.handler(p)
	return len(p), nil // Discard all data, assume the handler is taking care of it
}

func (rw *getData) Close() error {
	return nil
}

func (t *table) start(client gearman.Client, fn string, workload []byte,
	convert func([]byte) (optimus.Row, error)) {

	defer t.Stop()
	defer close(t.rows)

	data := &getData{handler: func(event []byte) {
		row, err := convert(event)
		if err != nil {
			t.err = err
			return
		}
		t.rows <- row
	}}
	warnings := gearmanUtils.NewBuffer()
	j, err := client.Submit(fn, workload, data, warnings)
	if err != nil {
		t.err = err
		return
	}
	state := j.Run()

	if state == job.Failed {
		t.err = fmt.Errorf("gearman job '%s' failed with warnings: %s", fn, warnings.Bytes())
	}
}

// New returns a new Table that outputs the worker data from a Gearman job. Converter should be a
// function that knows how to take a data event from Gearman and turn it into a Row.
func New(client gearman.Client, fn string, workload []byte,
	converter func([]byte) (optimus.Row, error)) optimus.Table {
	table := &table{
		rows: make(chan optimus.Row),
	}
	go table.start(client, fn, workload, converter)
	return table
}
