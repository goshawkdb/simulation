package parallel

import (
	"fmt"
	p "goshawkdb.io/simulation"
)

type SimpleBallot struct {
	*p.Txn
	abort     bool
	remaining int
	votes     map[*VarInstance]p.Outcome
	NoCompletionBallot
}

func NewSimpleBallot(txn *p.Txn, remaining, completions int) *SimpleBallot {
	return &SimpleBallot{
		Txn:       txn,
		abort:     false,
		remaining: remaining,
		votes:     make(map[*VarInstance]p.Outcome),
	}
}

func (sb *SimpleBallot) Vote(vi *VarInstance, decision p.Outcome) error {
	if decision == p.Abort {
		sb.abort = true
	}
	if _, found := sb.votes[vi]; found {
		return fmt.Errorf("VarInstance %v voted twice in txn %v", vi, sb.Txn)
	}
	sb.votes[vi] = decision
	sb.remaining--
	if sb.remaining < 0 {
		return fmt.Errorf("Too many votes for txn %v", sb.Txn)
	}
	return nil
}

func (sb *SimpleBallot) IsAbort() bool {
	return sb.abort
}

func (sb *SimpleBallot) IsStable() bool {
	return sb.abort || sb.remaining == 0
}

func (sb *SimpleBallot) AllVotesReceived() bool {
	return sb.remaining == 0
}
