package slice

import (
	"testing"

	"github.com/Clever/optimus/v4"
	"github.com/Clever/optimus/v4/tests"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New([]optimus.Row{
		{"thing1": []string{"1", "2"}},
		{"thing2": []string{"1", "2"}},
	}))
}
