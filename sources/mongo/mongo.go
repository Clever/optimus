package mongo

/*
 Package mongo adapts a mgo iterator (or anything that implements
 the interface) into a optimus.Table for consumption

 example:
   // Connect to mongo, get your collection then do the following:
	 iter := collection.Find(nil).Limit(100).Iter()
	 table := mongo.New(iter)
*/

import (
	"gopkg.in/Clever/optimus.v3"
)

// Iter simulates the mgo.Iter interface so we can remain independent
type Iter interface {
	Next(result interface{}) bool
	Err() error
}

// New returns a new Table that iterates over all the results of a mongo query.
func New(iter Iter) optimus.Table {
	s := &mongoSource{rows: make(chan optimus.Row)}
	go s.start(iter)
	return s
}

// mongoSource type matches the
type mongoSource struct {
	err     error
	rows    chan optimus.Row
	stopped bool
}

// start begins feeding rows into the rows channel
func (s *mongoSource) start(iter Iter) {
	defer s.Stop()
	defer close(s.rows)
	for !s.stopped {
		r := optimus.Row{}
		if !iter.Next(&r) {
			break
		}
		s.rows <- r
	}
	s.err = iter.Err()
}

// Rows returns the read side of the channel of optimus Rows from this mongo source
func (s *mongoSource) Rows() <-chan optimus.Row {
	return s.rows
}

// Err returns the last set err from the source
func (s *mongoSource) Err() error {
	return s.err
}

// Stop sets the stopped flag on the source
func (s *mongoSource) Stop() {
	s.stopped = true
}
