package slice

import (
	"github.com/azylman/optimus"
	"github.com/azylman/optimus/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New([]optimus.Row{
		{"thing1": []string{"1", "2"}},
		{"thing2": []string{"1", "2"}},
	}))
}
