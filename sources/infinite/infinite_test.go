package infinite

import (
	"gopkg.in/azylman/optimus.v1/tests"
	"testing"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New())
}
