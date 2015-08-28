package mongo

import (
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/tests"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Test Helpers
func newMongoSession(dbName string) mgo.Session {
	testURL, err := url.Parse(os.Getenv("MONGO_TEST_DB"))
	if err != nil {
		panic(err)
	}
	if testURL.Host == "" {
		testURL.Host = "localhost"
	}
	testURL.Scheme = "mongodb"
	testURL.Path = dbName
	session, err := mgo.Dial(testURL.String())
	if err != nil {
		panic(err)
	}
	return *session
}

func TestMongoStore(t *testing.T) {
	session := newMongoSession("TestMongoStore")
	db := session.DB("")
	defer db.DropDatabase()

	c := db.C("TestMongoCollection")

	// ids are added to make equality assertion simpler
	// not explicitly necessary for the mongoStore
	expectedRows := []optimus.Row{
		{"_id": bson.NewObjectId(), "field 1": "value 1", "field 2": "value 2"},
		{"_id": bson.NewObjectId(), "field 3": "value 3", "field 4": "value 4"},
	}

	store := NewStore(c)

	for _, row := range expectedRows {
		err := store.AddRow(row)
		assert.Nil(t, err)
	}

	assert.Equal(t, expectedRows, tests.GetRows(store))
}

func TestMongoGroupStore(t *testing.T) {
	session := newMongoSession("TestMongoGroupStore")
	db := session.DB("")
	defer db.DropDatabase()

	c := db.C("TestMongoGroupStore")

	input := []optimus.Row{
		{"key 1": "value 1", "key 2": "value 1"},
		{"key 1": "value 1", "key 2": "value 2"},
		{"key 1": "value 2", "key 2": "value 3"},
		{"key 1": "value 2", "key 2": "value 4"},
	}

	store := NewGroupedStore(c)

	for _, row := range input {
		err := store.AddRowToGroup(row, row["key 1"])
		assert.Nil(t, err)
	}

	expectedRows := map[string]optimus.Row{
		"value 1": {"_id": "value 1", "values": []optimus.Row{
			{"key 1": "value 1", "key 2": "value 1"},
			{"key 1": "value 1", "key 2": "value 2"},
		}},
		"value 2": {"_id": "value 2", "values": []optimus.Row{
			{"key 1": "value 2", "key 2": "value 3"},
			{"key 1": "value 2", "key 2": "value 4"},
		}},
	}

	actualRows := tests.GetRows(store)

	for _, actual := range actualRows {
		expected := expectedRows[actual["_id"].(string)]
		assert.Equal(t, expected["_id"], actual["_id"])

		// Types are dumb, and therefore so is this test
		values := actual["values"].([]interface{})
		for _, value := range values {
			row := value.(optimus.Row)
			assert.Equal(t, row["key 1"], expected["_id"])
		}
	}

}
