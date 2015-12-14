package serial

import (
	"github.com/msackman/gsim"
	p "goshawkdb.io/simulation"
	"math/big"
	"sort"
)

func FindAllHistories(txns ...*p.Txn) []*p.HistoryNode {
	sort.Sort(p.Txns(txns))
	txnsInterface := make([]interface{}, len(txns))
	for idx, txn := range txns {
		txnsInterface[idx] = txn
	}
	generator := gsim.NewSimplePermutation(txnsInterface)
	histories := make([]*p.HistoryNode, 0, len(txns))
	historyChan := make(chan *p.HistoryNode, 64)
	go func() {
		gsim.BuildPermutations(generator).ForEachPar(8192, consumer(historyChan))
		close(historyChan)
	}()
	for history := range historyChan {
		found := false
		for _, h := range histories {
			if found = h.Equal(history); found {
				break
			}
		}
		if !found {
			histories = append(histories, history)
		}
	}
	return histories
}

type consumer chan<- *p.HistoryNode

func (c consumer) Clone() gsim.PermutationConsumer { return c }

func (c consumer) Consume(n *big.Int, perm []interface{}) {
	txns := make([]*p.Txn, len(perm))
	for idx, txn := range perm {
		txns[idx] = txn.(*p.Txn)
	}

	var historyHead, historyTail *p.HistoryNode
	state := NewEmptyState()
	for _, txn := range txns {
		if outcome := evalTxn(txn, state); outcome == p.Commit {
			historyTail = p.NewHistoryNode(historyTail, txn)
			if historyHead == nil {
				historyHead = historyTail
			}
		}
	}
	c <- historyHead
}

func evalTxn(txn *p.Txn, state state) p.Outcome {
	abort := false
	for _, action := range txn.Actions {
		if action.IsRead() {
			vvv := state.getVarVersionValue(action.Var)
			if action.ReadVersion != vvv.Version {
				abort = true
				break
			}
		}
	}
	if abort {
		return p.Abort
	}

	for _, action := range txn.Actions {
		vvv := state.getVarVersionValue(action.Var)
		if action.IsWrite() {
			vvv.Value = action.WroteValue
			vvv.Version = txn.ID
		}
	}
	return p.Commit
}

type state map[p.Var]*p.VarVersionValue

func NewEmptyState() state {
	return make(map[p.Var]*p.VarVersionValue)
}

func (s state) getVarVersionValue(v p.Var) *p.VarVersionValue {
	if vvv, found := s[v]; found {
		return vvv
	} else {
		vvv := &p.VarVersionValue{
			Var:     v,
			Version: 0,
			Value:   nil,
		}
		s[v] = vvv
		return vvv
	}
}
