package gearman

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/Clever/gearman.v1/job"
	"gopkg.in/Clever/gearman.v1/packet"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/tests"
	"io"
	"testing"
)

type mockClient struct {
	*mock.Mock
	chans []chan *packet.Packet
}

func (c *mockClient) Close() error {
	return nil
}

func (c *mockClient) Submit(fn string, payload []byte, data, warnings io.WriteCloser) (job.Job, error) {
	_ = c.Mock.Called(fn, payload, data, warnings)
	packetChan := make(chan *packet.Packet)
	c.chans = append(c.chans, packetChan)
	j := job.New("", data, warnings, packetChan)
	return j, nil
}

func handlePacket(handle string, kind int, arguments [][]byte) *packet.Packet {
	if arguments == nil {
		arguments = [][]byte{}
	}
	arguments = append([][]byte{[]byte(handle)}, arguments...)
	return &packet.Packet{
		Type:      packet.Type(kind),
		Arguments: arguments,
	}
}

func TestGearmanSource(t *testing.T) {
	c := &mockClient{Mock: &mock.Mock{}, chans: []chan *packet.Packet{}}
	c.On("Submit", "function", []byte("workload"), mock.Anything, mock.Anything).Return(nil, nil).Once()
	numCalls := 0
	table := New(c, "function", []byte("workload"), func(in []byte) (optimus.Row, error) {
		numCalls++
		assert.Equal(t, in, []byte(fmt.Sprintf("%d", numCalls)))
		return optimus.Row{"field1": fmt.Sprintf("value%d", numCalls)}, nil
	})
	expected := []optimus.Row{
		{"field1": "value1"},
		{"field1": "value2"},
	}
	go func() {
		packets := c.chans[0]
		packets <- handlePacket("", packet.WorkData, [][]byte{[]byte("1")})
		packets <- handlePacket("", packet.WorkData, [][]byte{[]byte("2")})
		packets <- handlePacket("", packet.WorkComplete, nil)
	}()
	assert.Equal(t, expected, tests.GetRows(table))
	assert.Nil(t, table.Err())
}

func TestGearmanSourceFail(t *testing.T) {
	c := &mockClient{Mock: &mock.Mock{}, chans: []chan *packet.Packet{}}
	c.On("Submit", "function", []byte("workload"), mock.Anything, mock.Anything).Return(nil, nil).Once()
	table := New(c, "function", []byte("workload"), func(in []byte) (optimus.Row, error) {
		t.Fatal("never expected converter to be called")
		return nil, nil
	})
	go func() {
		packets := c.chans[0]
		packets <- handlePacket("", packet.WorkWarning, [][]byte{[]byte("1")})
		packets <- handlePacket("", packet.WorkFail, nil)
	}()
	expected := []optimus.Row{}
	assert.Equal(t, expected, tests.GetRows(table))
	assert.EqualError(t, table.Err(), "gearman job 'function' failed with warnings: 1")
}
