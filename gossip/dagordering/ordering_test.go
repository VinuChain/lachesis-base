package dagordering

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fantom-foundation/lachesis-base/eventcheck"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

func TestEventsBuffer(t *testing.T) {
	for try := int64(0); try < 1000; try++ {
		testEventsBuffer(t, try)
	}
}

func testEventsBuffer(t *testing.T, try int64) {
	nodes := tdag.GenNodes(5)

	var ordered dag.Events
	r := rand.New(rand.NewSource(try))
	_ = tdag.ForEachRandEvent(nodes, 10, 3, r, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	checked := 0

	processed := make(map[hash.Event]dag.Event)
	limit := dag.Metric{
		Num:  idx.Event(len(ordered)),
		Size: uint64(ordered.Metric().Size),
	}
	buffer := New(limit, Callback{

		Process: func(e dag.Event) error {
			if _, ok := processed[e.ID()]; ok {
				t.Fatalf("%s already processed", e.String())
				return nil
			}
			for _, p := range e.Parents() {
				if _, ok := processed[p]; !ok {
					t.Fatalf("got %s before parent %s", e.String(), p.String())
					return nil
				}
			}
			processed[e.ID()] = e
			return nil
		},

		Released: func(e dag.Event, peer string, err error) {
			if err != nil {
				t.Fatalf("%s unexpectedly dropped with '%s'", e.String(), err)
			}
		},

		Exists: func(id hash.Event) bool {
			return processed[id] != nil
		},

		Get: func(id hash.Event) dag.Event {
			return processed[id]
		},

		Check: func(e dag.Event, parents dag.Events) error {
			checked++
			if e.Frame() != idx.Frame(e.Seq()) {
				return errors.New("malformed event frame")
			}
			return nil
		},
	})

	// shuffle events
	for _, rnd := range r.Perm(len(ordered)) {
		e := ordered[rnd]
		buffer.PushEvent(e, "")
	}

	// everything is processed
	for _, e := range ordered {
		if _, ok := processed[e.ID()]; !ok {
			t.Fatal("event wasn't processed")
		}
	}
	if checked != len(processed) {
		t.Fatal("not all the events were checked")
	}
}

func TestEventsBufferReleasing(t *testing.T) {
	for try := int64(0); try < 100; try++ {
		testEventsBufferReleasing(t, 200, try)
	}
}

func testEventsBufferReleasing(t *testing.T, maxEvents int, try int64) {
	nodes := tdag.GenNodes(5)
	eventsPerNode := 1 + rand.Intn(maxEvents)/5

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, eventsPerNode, 3, rand.New(rand.NewSource(try)), tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	released := uint32(0)

	processed := make(map[hash.Event]dag.Event)
	var mutex sync.Mutex
	limit := dag.Metric{
		Num:  idx.Event(rand.Intn(maxEvents)),
		Size: uint64(rand.Intn(maxEvents * 100)),
	}
	buffer := New(limit, Callback{
		Process: func(e dag.Event) error {
			mutex.Lock()
			defer mutex.Unlock()
			if _, ok := processed[e.ID()]; ok {
				t.Fatalf("%s already processed", e.String())
				return nil
			}
			for _, p := range e.Parents() {
				if _, ok := processed[p]; !ok {
					t.Fatalf("got %s before parent %s", e.String(), p.String())
					return nil
				}
			}
			if rand.Intn(10) == 0 {
				return errors.New("testing error")
			}
			if rand.Intn(10) == 0 {
				time.Sleep(time.Microsecond * 100)
			}
			processed[e.ID()] = e
			return nil
		},

		Released: func(e dag.Event, peer string, err error) {
			mutex.Lock()
			defer mutex.Unlock()
			atomic.AddUint32(&released, 1)
		},

		Exists: func(e hash.Event) bool {
			mutex.Lock()
			defer mutex.Unlock()
			return processed[e] != nil
		},

		Get: func(e hash.Event) dag.Event {
			mutex.Lock()
			defer mutex.Unlock()
			return processed[e]
		},

		Check: func(e dag.Event, parents dag.Events) error {
			mutex.Lock()
			defer mutex.Unlock()
			if rand.Intn(10) == 0 {
				return errors.New("testing error")
			}
			if rand.Intn(10) == 0 {
				time.Sleep(time.Microsecond * 100)
			}
			return nil
		},
	})

	// duplicate some events
	ordered = append(ordered, ordered[:rand.Intn(len(ordered))]...)
	// shuffle events
	wg := sync.WaitGroup{}
	for _, rnd := range rand.Perm(len(ordered)) {
		e := ordered[rnd]
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer.PushEvent(e, "")
			if rand.Intn(10) == 0 {
				buffer.Clear()
			}
		}()
	}
	wg.Wait()
	buffer.Clear()

	// everything is released
	if uint32(len(ordered)) != released {
		t.Fatal("not all the events were released", len(ordered), released)
	}
}

// TestEventsBufferImportantEventNotSpilled verifies that when IsImportant is
// set, an important event is not evicted during a spill as long as there are
// non-important candidates to evict instead.
func TestEventsBufferImportantEventNotSpilled(t *testing.T) {
	nodes := tdag.GenNodes(3)

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, 10, 3, rand.New(rand.NewSource(42)), tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	if len(ordered) < 3 {
		t.Skip("need at least 3 events")
	}

	// The important event is the last one generated (it has no children in our
	// list so it will remain incomplete in the buffer).
	importantEvent := ordered[len(ordered)-1]
	importantID := importantEvent.ID()

	processed := make(map[hash.Event]dag.Event)
	var mu sync.Mutex

	spilledIDs := make(map[hash.Event]error)

	// Buffer limit of 1 forces aggressive spilling.
	limit := dag.Metric{Num: 1, Size: uint64(importantEvent.Size()) + 1}

	buffer := New(limit, Callback{
		Process: func(e dag.Event) error {
			mu.Lock()
			defer mu.Unlock()
			processed[e.ID()] = e
			return nil
		},
		Released: func(e dag.Event, peer string, err error) {
			if err == eventcheck.ErrSpilledEvent {
				mu.Lock()
				spilledIDs[e.ID()] = err
				mu.Unlock()
			}
		},
		Exists: func(id hash.Event) bool {
			mu.Lock()
			defer mu.Unlock()
			return processed[id] != nil
		},
		Get: func(id hash.Event) dag.Event {
			mu.Lock()
			defer mu.Unlock()
			return processed[id]
		},
		IsImportant: func(e dag.Event) bool {
			return e.ID() == importantID
		},
	})

	// Push all but the important event first, in order, so parents resolve.
	for _, e := range ordered[:len(ordered)-1] {
		buffer.PushEvent(e, "")
	}
	// Now push the important event (it will be incomplete because its parents
	// may not be present; regardless it should not be spilled while alternatives
	// exist).
	buffer.PushEvent(importantEvent, "")

	mu.Lock()
	_, wasSpilled := spilledIDs[importantID]
	mu.Unlock()

	if wasSpilled {
		t.Fatal("important event was spilled despite non-important candidates being available")
	}
}

// TestEventsBufferDuplicateSubmission verifies that submitting the same event
// twice (rapidly) does not cause incorrect duplicate detection for distinct
// events. The second submission of the same event must receive an error, while
// other events are unaffected.
func TestEventsBufferDuplicateSubmission(t *testing.T) {
	nodes := tdag.GenNodes(2)

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, 5, 2, rand.New(rand.NewSource(7)), tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	if len(ordered) < 2 {
		t.Skip("need at least 2 events")
	}

	processed := make(map[hash.Event]int)
	var mu sync.Mutex
	duplicateErrors := uint32(0)

	limit := dag.Metric{
		Num:  idx.Event(len(ordered) + 10),
		Size: uint64(ordered.Metric().Size) * 2,
	}

	buffer := New(limit, Callback{
		Process: func(e dag.Event) error {
			mu.Lock()
			defer mu.Unlock()
			processed[e.ID()]++
			return nil
		},
		Released: func(e dag.Event, peer string, err error) {
			if err == eventcheck.ErrDuplicateEvent || err == eventcheck.ErrAlreadyConnectedEvent {
				atomic.AddUint32(&duplicateErrors, 1)
			}
		},
		Exists: func(id hash.Event) bool {
			mu.Lock()
			defer mu.Unlock()
			return processed[id] > 0
		},
		Get: func(id hash.Event) dag.Event {
			mu.Lock()
			defer mu.Unlock()
			if processed[id] > 0 {
				for _, e := range ordered {
					if e.ID() == id {
						return e
					}
				}
			}
			return nil
		},
	})

	// Push all events in order first.
	for _, e := range ordered {
		buffer.PushEvent(e, "")
	}

	// Re-submit the first event — should trigger duplicate detection.
	dup := ordered[0]
	buffer.PushEvent(dup, "")

	mu.Lock()
	for _, e := range ordered {
		if cnt := processed[e.ID()]; cnt > 1 {
			t.Errorf("event %v processed %d times, want 1", e.ID(), cnt)
		}
	}
	mu.Unlock()

	if duplicateErrors == 0 {
		t.Error("expected at least one duplicate error, got none")
	}
}
