package vecengine

import (
	"testing"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

// forkEvent is a minimal dag.Event whose SelfParent always returns nil.
// Setting seq > 1 would normally imply a self-parent exists in BaseEvent,
// so we use a hand-rolled struct to control SelfParent independently.
type forkEvent struct {
	creator idx.ValidatorID
	seq     idx.Event
	id      hash.Event
}

func (e *forkEvent) Epoch() idx.Epoch        { return 1 }
func (e *forkEvent) Seq() idx.Event          { return e.seq }
func (e *forkEvent) Frame() idx.Frame        { return 0 }
func (e *forkEvent) Creator() idx.ValidatorID { return e.creator }
func (e *forkEvent) Lamport() idx.Lamport    { return 0 }
func (e *forkEvent) Parents() hash.Events    { return nil }
func (e *forkEvent) SelfParent() *hash.Event { return nil } // always nil → triggers fork path when BranchIDLastSeq != 0
func (e *forkEvent) IsSelfParent(h hash.Event) bool { return false }
func (e *forkEvent) ID() hash.Event          { return e.id }
func (e *forkEvent) String() string          { return "" }
func (e *forkEvent) Size() int               { return 0 }

var _ dag.Event = (*forkEvent)(nil)

// buildMinimalEngine constructs an Engine with a single validator (index 0)
// and manually populated BranchesInfo, without any kvdb dependency.
// The returned engine is only suitable for calling fillGlobalBranchID.
func buildMinimalEngine(validatorID idx.ValidatorID) (*Engine, idx.Validator) {
	validators := pos.ArrayToValidators(
		[]idx.ValidatorID{validatorID},
		[]pos.Weight{1},
	)

	vi := &Engine{
		crit:          func(err error) { panic(err) },
		validators:    validators,
		validatorIdxs: validators.Idxs(),
	}

	vi.bi = newInitialBranchesInfo(validators)
	// Seed BranchIDLastSeq[0] so the "is it first event?" guard fails and
	// subsequent SelfParent()==nil calls always reach the fork allocation path.
	vi.bi.BranchIDLastSeq[0] = 1

	meIdx := idx.Validator(0)
	return vi, meIdx
}

// TestFillGlobalBranchID_CapLimitsByzantineGrowth is the RED test.
// It calls fillGlobalBranchID many more times than maxBranchesPerValidator
// for the same validator and asserts that the branch count stays bounded.
func TestFillGlobalBranchID_CapLimitsByzantineGrowth(t *testing.T) {
	creatorID := idx.ValidatorID(1)
	vi, meIdx := buildMinimalEngine(creatorID)

	const iterations = maxBranchesPerValidator*2 + 10

	for i := 0; i < iterations; i++ {
		var id [32]byte
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		e := &forkEvent{
			creator: creatorID,
			seq:     idx.Event(i + 2), // >1 so it looks non-genesis, but SelfParent() is nil
			id:      hash.Event(id),
		}
		_, err := vi.fillGlobalBranchID(e, meIdx)
		if err != nil {
			t.Fatalf("fillGlobalBranchID returned unexpected error at iteration %d: %v", i, err)
		}
	}

	gotBranches := len(vi.bi.BranchIDByCreators[meIdx])
	if gotBranches > maxBranchesPerValidator {
		t.Errorf(
			"branch count for Byzantine validator exceeds cap: got %d, want <= %d",
			gotBranches, maxBranchesPerValidator,
		)
	}

	// Also verify the global BranchIDCreatorIdxs slice is bounded.
	totalBranches := len(vi.bi.BranchIDCreatorIdxs)
	// Initial count = number of validators (1), plus at most maxBranchesPerValidator-1 forks.
	maxTotal := maxBranchesPerValidator
	if totalBranches > maxTotal {
		t.Errorf(
			"total BranchIDCreatorIdxs exceeds cap: got %d, want <= %d",
			totalBranches, maxTotal,
		)
	}
}

// TestFillGlobalBranchID_CapDoesNotRegressLastSeq verifies that when the cap is
// reached and an event with a lower seq arrives, BranchIDLastSeq is not
// overwritten with the smaller value. This pins the max-seq invariant that the
// self-parent sequence check in fillGlobalBranchID depends on.
func TestFillGlobalBranchID_CapDoesNotRegressLastSeq(t *testing.T) {
	creatorID := idx.ValidatorID(1)
	vi, meIdx := buildMinimalEngine(creatorID)

	// Fill the validator up to the cap with ascending seqs.
	highSeq := idx.Event(maxBranchesPerValidator + 100)
	for i := 0; i < maxBranchesPerValidator; i++ {
		var id [32]byte
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		e := &forkEvent{
			creator: creatorID,
			seq:     idx.Event(i + 2),
			id:      hash.Event(id),
		}
		if _, err := vi.fillGlobalBranchID(e, meIdx); err != nil {
			t.Fatalf("setup: unexpected error at iteration %d: %v", i, err)
		}
	}

	// Record the last-seq after filling.
	lastBranch := vi.bi.BranchIDByCreators[meIdx][len(vi.bi.BranchIDByCreators[meIdx])-1]
	vi.bi.BranchIDLastSeq[lastBranch] = highSeq

	// Now send an event with a lower seq — it must not reduce BranchIDLastSeq.
	var oldID [32]byte
	oldID[0] = 0xff
	lowSeqEvent := &forkEvent{
		creator: creatorID,
		seq:     2, // very low
		id:      hash.Event(oldID),
	}
	if _, err := vi.fillGlobalBranchID(lowSeqEvent, meIdx); err != nil {
		t.Fatalf("unexpected error on low-seq event: %v", err)
	}

	if got := vi.bi.BranchIDLastSeq[lastBranch]; got != highSeq {
		t.Errorf(
			"BranchIDLastSeq regressed: got %d, want %d (high-seq invariant violated)",
			got, highSeq,
		)
	}
}

// TestFillGlobalBranchID_HonestValidatorUnaffected verifies that a single honest
// validator (no forks) still gets exactly one branch and the cap is never triggered.
func TestFillGlobalBranchID_HonestValidatorUnaffected(t *testing.T) {
	creatorID := idx.ValidatorID(1)
	vi, meIdx := buildMinimalEngine(creatorID)

	// Reset: first event — BranchIDLastSeq is 0, so the "first event" path fires.
	vi.bi.BranchIDLastSeq[0] = 0

	var id [32]byte
	e := &forkEvent{
		creator: creatorID,
		seq:     1,
		id:      hash.Event(id),
	}
	branchID, err := vi.fillGlobalBranchID(e, meIdx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if branchID != meIdx {
		t.Errorf("honest first event: got branchID %d, want %d", branchID, meIdx)
	}
	if len(vi.bi.BranchIDByCreators[meIdx]) != 1 {
		t.Errorf("honest validator should have exactly 1 branch, got %d", len(vi.bi.BranchIDByCreators[meIdx]))
	}
}
