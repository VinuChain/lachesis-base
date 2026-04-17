package dagprocessor

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/utils/cachescale"
	"github.com/Fantom-foundation/lachesis-base/utils/datasemaphore"
)

func TestProcessor(t *testing.T) {
	for try := 0; try < 500; try++ {
		testProcessor(t)
	}
}

var maxGroupSize = dag.Metric{
	Num:  50,
	Size: 50 * 50,
}

func shuffleEventsIntoChunks(inEvents dag.Events) []dag.Events {
	if len(inEvents) == 0 {
		return nil
	}
	var chunks []dag.Events
	var lastChunk dag.Events
	var lastChunkSize dag.Metric
	for _, rnd := range rand.Perm(len(inEvents)) {
		e := inEvents[rnd]
		if rand.Intn(10) == 0 || lastChunkSize.Num+1 >= maxGroupSize.Num || lastChunkSize.Size+uint64(e.Size()) >= maxGroupSize.Size {
			chunks = append(chunks, lastChunk)
			lastChunk = dag.Events{}
		}
		lastChunk = append(lastChunk, e)
		lastChunkSize.Num++
		lastChunkSize.Size += uint64(e.Size())
	}
	chunks = append(chunks, lastChunk)
	return chunks
}

func testProcessor(t *testing.T) {
	nodes := tdag.GenNodes(5)

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, 10, 3, nil, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	limit := dag.Metric{
		Num:  idx.Event(len(ordered)),
		Size: uint64(ordered.Metric().Size),
	}
	semaphore := datasemaphore.New(limit, func(received dag.Metric, processing dag.Metric, releasing dag.Metric) {
		t.Fatal("events semaphore inconsistency")
	})
	config := DefaultConfig(cachescale.Identity)
	config.EventsBufferLimit = limit

	checked := 0

	highestLamport := idx.Lamport(0)
	processed := make(map[hash.Event]dag.Event)
	mu := sync.RWMutex{}
	processor := New(semaphore, config, Callback{
		Event: EventCallback{
			Process: func(e dag.Event) error {
				mu.Lock()
				defer mu.Unlock()
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
				if highestLamport < e.Lamport() {
					highestLamport = e.Lamport()
				}
				processed[e.ID()] = e
				return nil
			},

			Released: func(e dag.Event, peer string, err error) {
				if err != nil {
					t.Fatalf("%s unexpectedly dropped with '%s'", e.String(), err)
				}
			},

			Exists: func(e hash.Event) bool {
				mu.RLock()
				defer mu.RUnlock()
				return processed[e] != nil
			},

			Get: func(id hash.Event) dag.Event {
				mu.RLock()
				defer mu.RUnlock()
				return processed[id]
			},

			CheckParents: func(e dag.Event, parents dag.Events) error {
				mu.RLock()
				defer mu.RUnlock()
				checked++
				if e.Frame() != idx.Frame(e.Seq()) {
					return errors.New("malformed event frame")
				}
				return nil
			},
			CheckParentless: func(e dag.Event, checked func(err error)) {
				checked(nil)
			},
		},
		HighestLamport: func() idx.Lamport {
			return highestLamport
		},
	})
	// shuffle events
	chunks := shuffleEventsIntoChunks(ordered)

	// process events
	processor.Start()
	wg := sync.WaitGroup{}
	for _, chunk := range chunks {
		wg.Add(1)
		err := processor.Enqueue("", chunk, rand.Intn(2) == 0, func(events hash.Events) {}, func() {
			wg.Done()
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	processor.Stop()

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

// TestQuitDrainsAllInFlightEvents verifies that when Stop() is called while
// CheckParentless callbacks are still pending (i.e., events are in-flight in
// the checker worker but have not yet written to checkedC), the quit drain
// blocks until all expected results arrive and calls Released for every event.
//
// The buggy pattern uses `select { case res := <-checkedC: ...; default: exit }`
// which returns as soon as checkedC is momentarily empty, skipping Released
// callbacks and permanently leaking semaphore slots.
//
// The fixed pattern blocks unconditionally: `res := <-checkedC`. This is safe
// because all in-flight CheckParentless callbacks will eventually fire and write
// to checkedC (which is buffered to len(events), so writes never block).
func TestQuitDrainsAllInFlightEvents(t *testing.T) {
	const numEvents = 10

	nodes := tdag.GenNodes(2)
	var events dag.Events
	_ = tdag.ForEachRandEvent(nodes, numEvents/2, 1, nil, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) { events = append(events, e) },
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})
	if len(events) == 0 {
		t.Fatal("no events generated")
	}

	// taskStarted is closed by CheckParentless on its first call, proving the
	// checker task is running and the orderedInserter is blocked on checkedC.
	taskStarted := make(chan struct{})
	taskStartedOnce := sync.Once{}

	// gate delays CheckParentless result delivery until after Stop() is called.
	gate := make(chan struct{})

	limit := dag.Metric{Num: idx.Event(len(events) + 1), Size: uint64(len(events)+1) * 1000}
	semaphore := datasemaphore.New(limit, func(received dag.Metric, processing dag.Metric, releasing dag.Metric) {
		t.Error("events semaphore inconsistency")
	})
	config := DefaultConfig(cachescale.Identity)
	config.EventsBufferLimit = limit

	var released int32

	processor := New(semaphore, config, Callback{
		Event: EventCallback{
			Process: func(e dag.Event) error { return nil },
			Released: func(e dag.Event, peer string, err error) {
				atomic.AddInt32(&released, 1)
			},
			Exists:       func(e hash.Event) bool { return false },
			Get:          func(id hash.Event) dag.Event { return nil },
			CheckParents: func(e dag.Event, parents dag.Events) error { return nil },
			// CheckParentless is async: it signals taskStarted on first call so
			// the test knows the checker task is running, then waits for gate
			// before writing the result to checkedC. This guarantees checkedC
			// is empty when the quit drain begins — triggering the bug.
			CheckParentless: func(e dag.Event, checked func(err error)) {
				taskStartedOnce.Do(func() { close(taskStarted) })
				go func() {
					<-gate
					checked(nil)
				}()
			},
			IsImportant: func(e dag.Event) bool { return false },
		},
		HighestLamport: func() idx.Lamport { return idx.Lamport(100) },
	})

	processor.Start()

	enqueueDone := make(chan struct{})
	err := processor.Enqueue("peer0", events, false, nil, func() { close(enqueueDone) })
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Wait until the checker task has started processing events. At this point
	// the checker worker is running CheckParentless (which has not yet written
	// to checkedC), and the orderedInserter task is blocked in its select loop.
	select {
	case <-taskStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("checker task did not start within 5s")
	}

	// Call Stop() while checkedC is empty (all callbacks are behind the gate).
	// The orderedInserter task receives quit, enters the drain branch, and
	// — with the bug — exits immediately via the `default` arm without calling
	// Released. With the fix, it blocks on `res := <-checkedC` for each event.
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		processor.Stop()
	}()

	// Brief pause so quit propagates into the select and the drain loop begins,
	// then open the gate so callbacks can deliver results to checkedC.
	time.Sleep(20 * time.Millisecond)
	close(gate)

	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s — possible deadlock or drain stuck")
	}

	got := int(atomic.LoadInt32(&released))
	if got != len(events) {
		t.Fatalf("Released called %d times, want %d — semaphore slots leaked on quit", got, len(events))
	}
	_ = enqueueDone
}

func TestProcessorReleasing(t *testing.T) {
	for try := int64(0); try < 100; try++ {
		testProcessorReleasing(t, 200, try)
	}
}

func testProcessorReleasing(t *testing.T, maxEvents int, try int64) {
	nodes := tdag.GenNodes(5)

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, 10, 3, rand.New(rand.NewSource(try)), tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	limit := dag.Metric{
		Num:  idx.Event(rand.Intn(maxEvents)),
		Size: uint64(rand.Intn(maxEvents * 100)),
	}
	limitPlus1group := dag.Metric{
		Num:  limit.Num + maxGroupSize.Num,
		Size: limit.Size + maxGroupSize.Size,
	}
	semaphore := datasemaphore.New(limitPlus1group, func(received dag.Metric, processing dag.Metric, releasing dag.Metric) {
		t.Fatal("events semaphore inconsistency")
	})
	config := DefaultConfig(cachescale.Identity)
	config.EventsBufferLimit = limit

	released := uint32(0)

	highestLamport := idx.Lamport(0)
	processed := make(map[hash.Event]dag.Event)
	mu := sync.RWMutex{}
	processor := New(semaphore, config, Callback{
		Event: EventCallback{
			Process: func(e dag.Event) error {
				mu.Lock()
				defer mu.Unlock()
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
				if highestLamport < e.Lamport() {
					highestLamport = e.Lamport()
				}
				processed[e.ID()] = e
				return nil
			},

			Released: func(e dag.Event, peer string, err error) {
				mu.Lock()
				defer mu.Unlock()
				atomic.AddUint32(&released, 1)
			},

			Exists: func(e hash.Event) bool {
				mu.RLock()
				defer mu.RUnlock()
				return processed[e] != nil
			},

			Get: func(id hash.Event) dag.Event {
				mu.RLock()
				defer mu.RUnlock()
				return processed[id]
			},

			CheckParents: func(e dag.Event, parents dag.Events) error {
				if rand.Intn(10) == 0 {
					return errors.New("testing error")
				}
				if rand.Intn(10) == 0 {
					time.Sleep(time.Microsecond * 100)
				}
				return nil
			},
			CheckParentless: func(e dag.Event, checked func(err error)) {
				var err error
				if rand.Intn(10) == 0 {
					err = errors.New("testing error")
				}
				if rand.Intn(10) == 0 {
					time.Sleep(time.Microsecond * 100)
				}
				checked(err)
			},
		},
		HighestLamport: func() idx.Lamport {
			return highestLamport
		},
	})
	// duplicate some events
	ordered = append(ordered, ordered[:rand.Intn(len(ordered))]...)
	// shuffle events
	chunks := shuffleEventsIntoChunks(ordered)

	// process events
	processor.Start()
	wg := sync.WaitGroup{}
	for _, chunk := range chunks {
		wg.Add(1)
		err := processor.Enqueue("", chunk, rand.Intn(2) == 0, func(events hash.Events) {}, func() {
			wg.Done()
		})
		if err != nil {
			t.Fatal(err)
		}
		if rand.Intn(10) == 0 {
			processor.Clear()
		}
	}
	wg.Wait()
	processor.Clear()
	if processor.eventsSemaphore.Processing().Num != 0 {
		t.Fatal("not all the events were released", processor.eventsSemaphore.Processing().Num)
	}
	processor.Stop()

	// everything is released
	if uint32(len(ordered)) != released {
		t.Fatal("not all the events were released", len(ordered), released)
	}
}
