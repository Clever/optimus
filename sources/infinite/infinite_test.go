package infinite

import (
	"testing"

	"github.com/Clever/optimus/v4/tests"
)

func TestStop(t *testing.T) {
	tests.Stop(t, New())
}
