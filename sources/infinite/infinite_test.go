package infinite

import (
	"github.com/azylman/optimus/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New())
}
