package infinite

import (
	"gopkg.in/Clever/optimus.v3/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New())
}
