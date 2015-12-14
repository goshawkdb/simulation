package protocol

import (
	"fmt"
	sl "github.com/msackman/skiplist"
	"log"
	"sort"
	"strings"
)

// Var

type Var string

func (v Var) String() string {
	return string(v)
}

// Vars

type Vars []Var

func (vars Vars) Len() int           { return len(vars) }
func (vars Vars) Less(i, j int) bool { return vars[i] < vars[j] }
func (vars Vars) Swap(i, j int)      { vars[i], vars[j] = vars[j], vars[i] }

// VarAction

type VarAction struct {
	Var
	ReadVersion int
	WroteValue  interface{}
}

func ReadAction(v Var, ver int) *VarAction {
	return &VarAction{Var: v, ReadVersion: ver}
}

func WriteAction(v Var, value interface{}) *VarAction {
	return &VarAction{Var: v, WroteValue: value}
}

func (va *VarAction) IsRead() bool {
	return va.WroteValue == nil
}

func (va *VarAction) IsWrite() bool {
	return va.WroteValue != nil
}

func (va *VarAction) String() string {
	if va.IsRead() && va.IsWrite() {
		return fmt.Sprintf("r(%v)%v w(%v := %v)", va.Var, va.ReadVersion, va.Var, va.WroteValue)
	} else if va.IsRead() {
		return fmt.Sprintf("r(%v)%v", va.Var, va.ReadVersion)
	} else if va.IsWrite() {
		return fmt.Sprintf("w(%v := %v)", va.Var, va.WroteValue)
	} else {
		return fmt.Sprintf("noop(%v)%v", va.Var)
	}
}

// VarActions

type VarActions []*VarAction

func (vas VarActions) Len() int           { return len(vas) }
func (vas VarActions) Less(i, j int) bool { return vas[i].Var < vas[j].Var }
func (vas VarActions) Swap(i, j int)      { vas[i], vas[j] = vas[j], vas[i] }

// Txn

type Txn struct {
	ID           int
	Actions      VarActions
	VarToActions map[Var]VarActions
}

func NewTxn(id int, actions ...*VarAction) *Txn {
	vas := VarActions(make([]*VarAction, len(actions)))
	copy(vas, actions)
	sort.Sort(vas)
	vasMap := make(map[Var]VarActions, len(actions))
	for _, va := range vas {
		if list, found := vasMap[va.Var]; found {
			vasMap[va.Var] = append(list, va)
		} else {
			vasMap[va.Var] = []*VarAction{va}
		}
	}
	return &Txn{
		ID:           id,
		Actions:      vas,
		VarToActions: vasMap,
	}
}

type TxnContainer interface {
	GetTxn() *Txn
}

func (txn *Txn) GetTxn() *Txn {
	return txn
}

func (txn *Txn) LessThan(b sl.Comparable) bool {
	bTxn := b.(TxnContainer)
	if b == nil || bTxn == nil || bTxn.GetTxn() == nil {
		return false
	} else if txn == nil {
		return true
	} else {
		return txn.ID < bTxn.GetTxn().ID
	}
}

// deliberately only looks at ID
func (txn *Txn) Equal(b sl.Comparable) bool {
	bTxn := b.(TxnContainer)
	if txn == nil {
		return b == nil || bTxn == nil || bTxn.GetTxn() == nil
	} else {
		return b != nil && bTxn != nil && bTxn.GetTxn() != nil && txn.ID == bTxn.GetTxn().ID
	}
}

func (txn *Txn) String() string {
	return fmt.Sprintf("Txn %v %v", txn.ID, txn.Actions)
}

func (txn *Txn) Clone() *Txn {
	return NewTxn(txn.ID, txn.Actions...)
}

// Txns

type Txns []*Txn

func (txns Txns) Len() int           { return len(txns) }
func (txns Txns) Less(i, j int) bool { return txns[i].ID < txns[j].ID }
func (txns Txns) Swap(i, j int)      { txns[i], txns[j] = txns[j], txns[i] }

func (a Txns) Equal(b Txns) bool {
	if len(a) == len(b) {
		for idx := range a {
			if !a[idx].Equal(b[idx]) {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (txns Txns) String() string {
	strs := make([]string, len(txns))
	for idx, tr := range txns {
		strs[idx] = fmt.Sprintf("%v", tr)
	}
	return "[" + strings.Join(strs, ",\n\t") + "]\n"
}

// VarVersionValue

type VarVersionValue struct {
	Var
	Version int
	Value   interface{}
}

type VarVersionValues []*VarVersionValue

func (vvvs VarVersionValues) Len() int      { return len(vvvs) }
func (vvvs VarVersionValues) Swap(i, j int) { vvvs[i], vvvs[j] = vvvs[j], vvvs[i] }
func (vvvs VarVersionValues) Less(i, j int) bool {
	return vvvs[i].Var < vvvs[j].Var ||
		(vvvs[i].Var == vvvs[j].Var && vvvs[i].Version < vvvs[j].Version)
}

func (vvvs VarVersionValues) Sort() { sort.Sort(vvvs) }

func (vvv *VarVersionValue) Clone() *VarVersionValue {
	return &VarVersionValue{
		Var:     vvv.Var,
		Version: vvv.Version,
		Value:   vvv.Value,
	}
}

func (vvv *VarVersionValue) String() string {
	return fmt.Sprintf("%v(%v)=%v", vvv.Var, vvv.Version, vvv.Value)
}

func (a *VarVersionValue) Equal(b *VarVersionValue) bool {
	if a == b {
		return true
	}
	return a.Var == b.Var && a.Version == b.Version && a.Value == b.Value
}

// HistoryNode

type HistoryNode struct {
	CommittedTxn *Txn
	Next         []*HistoryNode
	Previous     []*HistoryNode
}

func NewHistoryNode(parent *HistoryNode, txn *Txn) *HistoryNode {
	node := &HistoryNode{
		CommittedTxn: txn,
	}
	if parent != nil {
		parent.AddEdgeTo(node)
	}
	return node
}

func (hnA *HistoryNode) Equal(hnB *HistoryNode) bool {
	if hnA == hnB {
		return true
	}
	if hnA == nil || hnB == nil {
		return false
	}
	if hnA.CommittedTxn.Equal(hnB.CommittedTxn) && len(hnA.Next) == len(hnB.Next) {
		for _, nextA := range hnA.Next {
			found := false
			for _, nextB := range hnB.Next {
				if found = nextA.Equal(nextB); found {
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	return false
}

func (hn *HistoryNode) AddEdgeTo(nodes ...*HistoryNode) {
	for _, node := range nodes {
		found := false
		for _, to := range hn.Next {
			if found = node == to; found {
				break
			}
		}
		if !found {
			hn.Next = append(hn.Next, node)
			node.Previous = append(node.Previous, hn)
		}
	}
}

func (hn *HistoryNode) RemoveEdgeTo(node *HistoryNode) {
	found := false
	for idx, n := range hn.Next {
		if found = n == node; found {
			hn.Next = append(hn.Next[:idx], hn.Next[idx+1:]...)
			break
		}
	}
	if !found {
		return
	}
	for idx, n := range node.Previous {
		if n == hn {
			node.Previous = append(node.Previous[:idx], node.Previous[idx+1:]...)
			return
		}
	}
}

func (hn *HistoryNode) Len() int {
	if hn == nil {
		return 0
	}
	switch len(hn.Next) {
	case 0:
		return 1
	case 1:
		return 1 + hn.Next[0].Len()
	default:
		log.Fatal("Len() called on non-serial history")
		return -1 // doesn't matter - unreachable
	}
}

func (hn *HistoryNode) String() string {
	return hn.string("\n") + "\n"
}

func (hn *HistoryNode) string(nl string) string {
	nextsStrs := make([]string, len(hn.Next))
	for idx, next := range hn.Next {
		nextsStrs[idx] = next.string(nl + "   ")
	}
	return fmt.Sprintf("HistoryNode%v  Committed Txn:%v    %v%v  Nexts: [%v   %v]",
		nl, nl, hn.CommittedTxn,
		nl, nl, strings.Join(nextsStrs, ";"))
}

// Outcome

type Outcome int

const (
	Commit Outcome = iota
	Abort          = iota
)

func (o Outcome) String() string {
	if o == Commit {
		return "Commited"
	} else {
		return "Aborted"
	}
}
