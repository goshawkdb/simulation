package engines

import (
	p "goshawkdb.io/simulation"
	par "goshawkdb.io/simulation/parallel"
)

// This is a daft engine that just aborts everything. Could not be
// simpler.
type AbortTxnEngine struct {
	par.NoCompletionTxnEngine
}

func (ate *AbortTxnEngine) Clone() par.TxnEngine { return ate }

func (ate *AbortTxnEngine) NewBallot(txn *p.Txn, remaining, completions int) par.Ballot {
	return par.NewSimpleBallot(txn, remaining, completions)
}

func (ate *AbortTxnEngine) NewEngineVar(vi *par.VarInstance, varState *p.VarVersionValue) par.EngineVar {
	return &AbortTxnEngineVar{
		VarInstance: vi,
		historyHead: p.NewHistoryNode(nil, nil),
	}
}

type AbortTxnEngineVar struct {
	*par.VarInstance
	historyHead *p.HistoryNode
	par.NoCompletionTxnEngineVar
}

func (ater *AbortTxnEngineVar) TxnReceived(txn *p.Txn, ballot par.Ballot) error {
	return ballot.(*par.SimpleBallot).Vote(ater.VarInstance, p.Abort)
}

func (ater *AbortTxnEngineVar) TxnVotesReceived(txn *p.Txn, ballot par.Ballot) error {
	return nil
}

func (ater *AbortTxnEngineVar) CommitHistory() *p.HistoryNode {
	return ater.historyHead
}
