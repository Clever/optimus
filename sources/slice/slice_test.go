package slice

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New([]getl.Row{
		{"thing1": []string{"1", "2"}},
		{"thing2": []string{"1", "2"}},
	}))
}
