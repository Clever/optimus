package slice

import (
	"gopkg.in/azylman/optimus.v1"
	"gopkg.in/azylman/optimus.v1/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New([]optimus.Row{
		{"thing1": []string{"1", "2"}},
		{"thing2": []string{"1", "2"}},
	}))
}
