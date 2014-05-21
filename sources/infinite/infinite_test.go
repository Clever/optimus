package infinite

import (
	"github.com/azylman/getl/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New())
}
