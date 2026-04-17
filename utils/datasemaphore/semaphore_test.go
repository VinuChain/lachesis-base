package datasemaphore

import (
	"testing"
	"time"

	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

// TestAvailableAfterTerminate verifies that Available() returns the zero metric
// after Terminate() is called, rather than an underflowed near-MaxUint value.
func TestAvailableAfterTerminate(t *testing.T) {
	maxProcessing := dag.Metric{Num: idx.Event(10), Size: 1000}
	s := New(maxProcessing, nil)

	// Acquire some capacity so processing > 0 when we terminate.
	acquired := s.TryAcquire(dag.Metric{Num: 3, Size: 300})
	if !acquired {
		t.Fatal("expected TryAcquire to succeed before termination")
	}

	// Terminate zeroes maxProcessing while processing is still non-zero.
	s.Terminate()

	got := s.Available()
	want := dag.Metric{}
	if got != want {
		t.Errorf("Available() after Terminate() = %v, want %v (zero metric); underflow detected", got, want)
	}
}

// TestAvailableUnderflowClamp verifies that Available() clamps to zero rather
// than wrapping around when processing somehow exceeds maxProcessing.
func TestAvailableUnderflowClamp(t *testing.T) {
	maxProcessing := dag.Metric{Num: idx.Event(10), Size: 1000}
	s := New(maxProcessing, nil)

	// Acquire the full capacity.
	acquired := s.TryAcquire(maxProcessing)
	if !acquired {
		t.Fatal("expected TryAcquire to succeed for full capacity")
	}

	// Simulate a condition where processing exceeds maxProcessing by calling
	// Release with a smaller value than was acquired, then calling Terminate
	// which zeroes maxProcessing while processing remains positive.
	s.Release(dag.Metric{Num: 5, Size: 500})
	// Now processing = {5, 500}, maxProcessing = {10, 1000}; normal so far.

	// Terminate: maxProcessing -> {0,0}, processing stays at {5, 500}.
	s.Terminate()

	got := s.Available()
	want := dag.Metric{}
	if got != want {
		t.Errorf("Available() after Terminate() with residual processing = %v, want %v; underflow detected", got, want)
	}
}

// TestAvailableNormal verifies that Available() still returns the correct value
// under normal (non-terminated) operation.
func TestAvailableNormal(t *testing.T) {
	maxProcessing := dag.Metric{Num: idx.Event(10), Size: 1000}
	s := New(maxProcessing, nil)

	weight := dag.Metric{Num: 3, Size: 300}
	s.TryAcquire(weight)

	got := s.Available()
	want := dag.Metric{Num: 7, Size: 700}
	if got != want {
		t.Errorf("Available() = %v, want %v", got, want)
	}
}

// TestAvailableFullCapacityWhenIdle verifies that Available() equals maxProcessing
// when nothing has been acquired.
func TestAvailableFullCapacityWhenIdle(t *testing.T) {
	maxProcessing := dag.Metric{Num: idx.Event(5), Size: 500}
	s := New(maxProcessing, nil)

	got := s.Available()
	if got != maxProcessing {
		t.Errorf("Available() on idle semaphore = %v, want %v", got, maxProcessing)
	}
}

// TestAvailableAfterTerminateNeverAcquired checks that Available() returns zero
// even when nothing was ever acquired before Terminate().
func TestAvailableAfterTerminateNeverAcquired(t *testing.T) {
	maxProcessing := dag.Metric{Num: idx.Event(100), Size: 99999}
	s := New(maxProcessing, nil)

	s.Terminate()

	got := s.Available()
	want := dag.Metric{}
	if got != want {
		t.Errorf("Available() after Terminate() on idle semaphore = %v, want %v", got, want)
	}
}

// TestTerminateUnblocksAcquire verifies that Terminate() broadcasts and causes
// in-flight Acquire calls to unblock (they should return false).
func TestTerminateUnblocksAcquire(t *testing.T) {
	maxProcessing := dag.Metric{Num: idx.Event(1), Size: 100}
	s := New(maxProcessing, nil)

	// Fill the semaphore so the next Acquire will block.
	s.TryAcquire(maxProcessing)

	done := make(chan bool, 1)
	go func() {
		result := s.Acquire(dag.Metric{Num: 1, Size: 1}, 5*time.Second)
		done <- result
	}()

	// Give the goroutine time to start blocking.
	time.Sleep(10 * time.Millisecond)
	s.Terminate()

	select {
	case result := <-done:
		if result {
			t.Error("Acquire should return false after Terminate")
		}
	case <-time.After(2 * time.Second):
		t.Error("Acquire did not unblock after Terminate within timeout")
	}
}
