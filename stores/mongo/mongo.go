package mongo

import (
	"errors"
	"sync"

	"github.com/Clever/optimus/stores"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func NewStore(collection *mgo.Collection) stores.Store {
	s := &mongoStore{collection: collection, mutex: sync.Mutex{}, rows: make(chan optimus.Row)}
	return s
}

func NewGroupedStore(collection *mgo.Collection) stores.GroupedStore {
	s := &mongoStore{collection: collection, mutex: sync.Mutex{}, rows: make(chan optimus.Row)}
	return s
}

type mongoStore struct {
	collection *mgo.Collection
	mutex      sync.Mutex
	err        error
	rows       chan optimus.Row
	started    bool
	stopped    bool
}

func (store *mongoStore) AddRow(row optimus.Row) error {
	if store.started {
		return errors.New("Add While Streaming")
	}
	store.mutex.Lock()
	err := store.collection.Insert(bson.M(row))
	store.mutex.Unlock()
	return err
}

func (store *mongoStore) AddRowToGroup(row optimus.Row, groupKey interface{}) error {
	if store.started {
		return errors.New("Add While Streaming")
	}
	store.mutex.Lock()
	_, err := store.collection.Upsert(bson.M{"_id": groupKey}, bson.M{"$push": bson.M{"values": row}})
	store.mutex.Unlock()
	return err
}

func (store *mongoStore) Rows() <-chan optimus.Row {
	if !store.started {
		iter := store.collection.Find(nil).Iter()
		go store.start(iter)
		store.started = true
	}
	return store.rows
}

func (store *mongoStore) Err() error {
	return store.err
}

func (store *mongoStore) Stop() {
	store.stopped = true
}

func (store *mongoStore) start(iter *mgo.Iter) {
	defer store.Stop()
	defer close(store.rows)
	for !store.stopped {
		r := optimus.Row{}
		if !iter.Next(&r) {
			break
		}
		store.rows <- r
	}
	store.err = iter.Err()
}
