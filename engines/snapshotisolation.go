package engines

import (
	p "goshawkdb.io/simulation"
	par "goshawkdb.io/simulation/parallel"
)

// This is a daft implementation of snapshot isolation, which is wrong
// because there is no global knowledge of orderings. So things will
// go wrong quickly. I.e. this isn't even snapshot isolation really.
type SITxnEngine struct {
	par.NoCompletionTxnEngine
}

func (site *SITxnEngine) Clone() par.TxnEngine { return site }

func (site *SITxnEngine) NewBallot(txn *p.Txn, remaining, completions int) par.Ballot {
	return par.NewSimpleBallot(txn, remaining, completions)
}

func (site *SITxnEngine) NewEngineVar(vi *par.VarInstance, varState *p.VarVersionValue) par.EngineVar {
	historyHead := p.NewHistoryNode(nil, nil)
	return &SITxnEngineVar{
		VarInstance: vi,
		varState:    varState,
		historyHead: historyHead,
		historyTail: historyHead,
	}
}

type SITxnEngineVar struct {
	*par.VarInstance
	varState    *p.VarVersionValue
	historyHead *p.HistoryNode
	historyTail *p.HistoryNode
	par.NoCompletionTxnEngineVar
}

func (siter *SITxnEngineVar) TxnReceived(txn *p.Txn, ballot par.Ballot) error {
	if actions, found := txn.VarToActions[siter.varState.Var]; found {
		for _, action := range actions {
			if action.IsRead() && action.ReadVersion != siter.varState.Version {
				return ballot.(*par.SimpleBallot).Vote(siter.VarInstance, p.Abort)
			}
		}
	}

	return ballot.(*par.SimpleBallot).Vote(siter.VarInstance, p.Commit)

}

func (siter *SITxnEngineVar) TxnVotesReceived(txn *p.Txn, ballot par.Ballot) error {
	if ballot.IsAbort() {
		return nil
	}

	wrote := false
	if actions, found := txn.VarToActions[siter.varState.Var]; found {
		for _, action := range actions {
			if action.IsWrite() {
				wrote = true
				siter.varState.Version++
				siter.varState.Value = action.WroteValue
			}
		}
	}

	if wrote && len(siter.historyTail.Next) == 0 {
		// we're a write, but there've been no reads of the last write
		siter.historyTail = p.NewHistoryNode(siter.historyTail, txn)

	} else if wrote {
		// there have been reads
		reads := siter.historyTail.Next
		siter.historyTail = p.NewHistoryNode(nil, txn)
		for _, read := range reads {
			read.AddEdgeTo(siter.historyTail)
		}

	} else {
		// we only read.
		p.NewHistoryNode(siter.historyHead, txn)
	}

	return nil
}

func (siter *SITxnEngineVar) CommitHistory() *p.HistoryNode {
	return siter.historyHead
}
