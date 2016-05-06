package parallel

import (
	"fmt"
	sl "github.com/msackman/skiplist"
	//"strings"
	p "goshawkdb.io/simulation"
)

// This code requires all the partial histories to be DAGs and
// requires the serial histories to be completely linear. If there are
// cycles in the partial histories, this will not work.
func (c *consumer) verifyHistoryIsSerial(state *state) (int, int, bool, error) {
	worklist := []*p.HistoryNode{}
	failed, prefixLen := false, 0

	varHistories, err := mergeCommonVars(state.viState)
	if err != nil {
		return 0, 0, false, err
	}

	// For each possible serial history (in ascending order of length)...
	for serialHistoryLen, serialHistories := range c.serialHistories {
		for _, serial := range serialHistories {
			failed, prefixLen = false, 0
			// ...we check it against all the partial histories, to find if
			// there's a serial history which is not violated by ALL of the
			// partial histories.
			for varInstance, history := range varHistories {
				if history == nil {
					return 0, 0, false, fmt.Errorf("Nil history for %v\n", varInstance)
				}
				reached := make(map[*p.HistoryNode]bool, serialHistoryLen)
				worklist = addToWorkList(worklist[:0], reached, history)

				serialNode, serialCovered := serial, 0
				for len(worklist) != 0 {
					if _, found := serialNode.CommittedTxn.VarToActions[varInstance.Var]; found {
						// this varInstance was involved in this txn
						found := false
						for idx, item := range worklist {
							if serialNode.CommittedTxn.Compare(item.CommittedTxn) == sl.EQ {
								found = true
								worklist = append(worklist[:idx], worklist[idx+1:]...)
								reached[item] = true
								for _, next := range item.Next {
									if allPreviousReached(next, reached) {
										worklist = addToWorkList(worklist, reached, next)
									}
								}
								break
							}
						}
						if !found {
							// guaranteed worklist is not empty. So we'll fail below.
							break
						}
					}
					serialCovered++
					if len(serialNode.Next) == 0 {
						break
					} else {
						serialNode = serialNode.Next[0]
					}
				}

				// If we have items left over in worklist then we've failed this serial history.
				failed = failed || len(worklist) != 0
				if failed { // don't merge with the above. We still need failed to be set.
					// the most recently tried partial history violated the
					// serial history. Move to the next serial history.
					break
				} else if serialCovered > prefixLen {
					// We only need one partial history to reach the end of
					// the serial history in order to claim it's not a
					// prefix.
					prefixLen = serialCovered
				}
			}

			if !failed {
				commitCount := prefixLen
				abortCount := len(c.txnToVIs) - commitCount
				return commitCount, abortCount, prefixLen != serialHistoryLen, nil
			}
		}
	}

	return 0, 0, false, fmt.Errorf("Unable to find history")
}

func addToWorkList(worklist []*p.HistoryNode, reached map[*p.HistoryNode]bool, item *p.HistoryNode) []*p.HistoryNode {
	if item.CommittedTxn == nil || item.CommittedTxn.ID == 0 {
		reached[item] = true
		for _, next := range item.Next {
			if allPreviousReached(next, reached) {
				worklist = addToWorkList(worklist, reached, next)
			}
		}
	} else {
		worklist = append(worklist, item)
	}
	return worklist
}

func allPreviousReached(node *p.HistoryNode, reached map[*p.HistoryNode]bool) bool {
	for _, prev := range node.Previous {
		if _, found := reached[prev]; !found {
			return false
		}
	}
	return true
}

func mergeCommonVars(viStates map[*VarInstance]*viState) (map[*VarInstance]*p.HistoryNode, error) {
	histories := make(map[*VarInstance]*p.HistoryNode)

	dups := make(map[p.Var]*[]*VarInstance)
	for vi := range viStates {
		if listPtr, found := dups[vi.Var]; found {
			*listPtr = append(*listPtr, vi)
		} else {
			list := []*VarInstance{vi}
			dups[vi.Var] = &list
		}
	}

	for v, listPtr := range dups {
		if len(*listPtr) == 1 {
			vi := (*listPtr)[0]
			histories[vi] = viStates[vi].CommitHistory()
			continue
		} else {
			winnerVI := NewVarInstance("", v)
			hns := NewHistoryNodes()
			for _, vi := range *listPtr {
				winnerVI.id = fmt.Sprintf("%v|%v", winnerVI.id, vi.id)
				history := viStates[vi].CommitHistory()
				// fmt.Printf("Merging in %v: %v\n", vi.id, history)
				hns.Merge(history)
			}
			history, err := hns.CommitHistory()
			if err != nil {
				return nil, err
			}
			histories[winnerVI] = history
		}
	}
	// fmt.Println(histories)

	return histories, nil
}

type historyNodes struct {
	nodes map[*p.Txn]*p.HistoryNode
	roots map[*p.HistoryNode]bool
}

func NewHistoryNodes() *historyNodes {
	return &historyNodes{
		nodes: make(map[*p.Txn]*p.HistoryNode),
		roots: make(map[*p.HistoryNode]bool),
	}
}

func (hns historyNodes) Get(txn *p.Txn) (*p.HistoryNode, bool) {
	if hn, found := hns.nodes[txn]; found {
		return hn, found
	}
	for t, hn := range hns.nodes {
		if t.Compare(txn) == sl.EQ {
			return hn, true
		}
	}
	return nil, false
}

func (hns historyNodes) Merge(hn *p.HistoryNode) {
	if _, found := hns.Get(hn.CommittedTxn); !found {
		hns.roots[hn] = true
	}

	visited := make(map[*p.HistoryNode]bool)
	worklist := []*p.HistoryNode{hn}
	for len(worklist) > 0 {
		cur := worklist[0]
		worklist = worklist[1:]
		visited[cur] = true

		existing, found := hns.Get(cur.CommittedTxn)
		if found {
			// need to add Nexts
			for _, next := range cur.Next {
				// do we already have this next on the hn?
				alreadyInNext := false
				for _, existingNext := range existing.Next {
					if alreadyInNext = next.CommittedTxn.Compare(existingNext.CommittedTxn) == sl.EQ; alreadyInNext {
						break
					}
				}
				if !alreadyInNext {
					// ok, it's not already pointing to next, but is next still already known?
					if existingNext, found := hns.Get(next.CommittedTxn); found {
						existing.Next = append(existing.Next, existingNext)
					} else {
						newNext := p.NewHistoryNode(nil, next.CommittedTxn)
						existing.Next = append(existing.Next, newNext)
						hns.nodes[next.CommittedTxn] = newNext
					}
				}
			}
		} else {
			existing = cur
			hns.nodes[cur.CommittedTxn] = cur
			cur.Previous = cur.Previous[:0]
			if cur != hn {
				delete(hns.roots, cur)
			}
		}

		for _, next := range cur.Next {
			if !visited[next] {
				worklist = append(worklist, next)
			}
		}
	}
}

func (hns historyNodes) CommitHistory() (*p.HistoryNode, error) {
	if len(hns.roots) != 1 {
		return nil, fmt.Errorf("Require 1 root, but found:%v", hns.roots)
	}
	var root *p.HistoryNode
	for hn := range hns.roots {
		root = hn
	}
	worklist := []*p.HistoryNode{root}
	visited := make(map[*p.HistoryNode]bool)
	for len(worklist) > 0 {
		cur := worklist[0]
		worklist = worklist[1:]
		visited[cur] = true
		for _, next := range cur.Next {
			next.Previous = append(next.Previous, cur)
			if !visited[next] {
				worklist = append(worklist, next)
			}
		}
	}
	return root, nil
}
