package saga_test

import (
	"testing"

	"github.com/floatdrop/grpcproc/saga"
	"github.com/floatdrop/grpcproc/saga/sagatest"
)

func TestMemoryStore(t *testing.T) {
	sagatest.Store(t, func(*testing.T) saga.Store { return saga.Memory() })
}
