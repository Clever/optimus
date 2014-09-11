package infinite

import (
	"gopkg.in/azylman/optimus.v2/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New())
}
