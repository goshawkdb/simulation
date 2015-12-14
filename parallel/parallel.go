package parallel

import (
	"fmt"
	"github.com/msackman/gsim"
	p "goshawkdb.io/simulation"
	"log"
	"math/big"
	"math/rand"
	"sort"
)

type StartingNodeModifier func([]*gsim.GraphNode, map[*p.Txn]map[*VarInstance][]*gsim.GraphNode) []*gsim.GraphNode

func ValidateAllHistories(engine TxnEngine, txns []*p.Txn, vis []*VarInstance, snm StartingNodeModifier, majorityVoting bool, serialHistories []*p.HistoryNode) *Histogram {
	con, gen := prepareSimulation(engine, txns, vis, snm, majorityVoting, serialHistories)
	gsim.BuildPermutations(gen).ForEachPar(8192, con)
	return con.histogram.GatherAll()
}

func ValidateHistory(engine TxnEngine, txns []*p.Txn, vis []*VarInstance, snm StartingNodeModifier, majorityVoting bool, serialHistories []*p.HistoryNode, num *big.Int) *Histogram {
	con, gen := prepareSimulation(engine, txns, vis, snm, majorityVoting, serialHistories)
	con.Consume(num, gsim.BuildPermutations(gen).Permutation(num))
	return con.histogram.GatherAll()
}

func prepareSimulation(engine TxnEngine, txns []*p.Txn, vis []*VarInstance, snm StartingNodeModifier, majorityVoting bool, serialHistories []*p.HistoryNode) (*consumer, gsim.OptionGenerator) {
	sort.Sort(p.Txns(txns))
	VarInstances(vis).Sort()

	generator, c := buildOptionGenerator(txns, vis, snm, majorityVoting, engine.NeedsCompletionNodes())

	historiesByLen := make([][]*p.HistoryNode, len(serialHistories)+1)
	for _, history := range serialHistories {
		l := history.Len()
		histsAtL := historiesByLen[l]
		if histsAtL == nil {
			histsAtL = []*p.HistoryNode{history}
		} else {
			histsAtL = append(histsAtL, history)
		}
		historiesByLen[l] = histsAtL
	}

	c.serialHistories = historiesByLen
	c.varInstances = vis
	c.engine = engine
	rng := rand.New(rand.NewSource(0))
	c.histogram = NewHistogram(make(chan []*Histogram, 1), rng)
	return c, generator
}

func buildOptionGenerator(txns []*p.Txn, vis []*VarInstance, snm StartingNodeModifier, majorityVoting, needsCompletionNodes bool) (gsim.OptionGenerator, *consumer) {
	varToVIs := make(map[p.Var]*VarInstances)
	for _, vi := range vis {
		if visPtr, found := varToVIs[vi.Var]; found {
			*visPtr = append(*visPtr, vi)
		} else {
			viList := VarInstances([]*VarInstance{vi})
			varToVIs[vi.Var] = &viList
		}
	}
	for _, visPtr := range varToVIs {
		visPtr.Sort()
	}

	txnToVIs := make(map[*p.Txn]VarInstances)
	for _, txn := range txns {
		vis := VarInstances(make([]*VarInstance, 0, len(txn.VarToActions)))
		for v, _ := range txn.VarToActions {
			vis = append(vis, *varToVIs[v]...)
		}
		vis.Sort()
		txnToVIs[txn] = vis
	}

	startingNodes := []*gsim.GraphNode{}
	startingNodesMap := make(map[*p.Txn]map[*VarInstance][]*gsim.GraphNode)

	for _, txn := range txns {
		nodesMap := make(map[*VarInstance][]*gsim.GraphNode)
		startingNodesMap[txn] = nodesMap
		vis := txnToVIs[txn]
		txnStartingNodes := []*gsim.GraphNode{}
		txnVoteReceivedNodes := []*gsim.GraphNode{}
		txnCompletionReceivedNodes := []*gsim.GraphNode{}
		for _, vi := range vis {
			viNodes := make([]*gsim.GraphNode, 2, 3)
			startingNode := gsim.NewGraphNode(&txnReceived{
				txn: txn,
				vi:  vi,
			})
			txnStartingNodes = append(txnStartingNodes, startingNode)
			viNodes[0] = startingNode
			voteReceivedNode := gsim.NewGraphNode(&txnVotesReceived{
				txn: txn,
				vi:  vi,
			})
			txnVoteReceivedNodes = append(txnVoteReceivedNodes, voteReceivedNode)
			viNodes[1] = voteReceivedNode
			if needsCompletionNodes {
				completionReceivedNode := gsim.NewGraphNode(&txnGloballyCompleteReceived{
					txn: txn,
					vi:  vi,
				})
				txnCompletionReceivedNodes = append(txnCompletionReceivedNodes, completionReceivedNode)
				viNodes = append(viNodes, completionReceivedNode)
			}
			nodesMap[vi] = viNodes
		}

		var startingCallback, votingCallback gsim.GraphNodeCallback
		if majorityVoting {
			startingCallback = NewInhibitMajorityCallback(txnStartingNodes...)
			votingCallback = NewAvailableMajorityCallback(txnStartingNodes...)
		} else {
			votingCallback = gsim.NewAvailableAllCallback(txnStartingNodes...)
		}

		// The vote for this txn can not be received by anyone until at
		// least all the VIs involved have received this txn.
		for _, startingNode := range txnStartingNodes {
			for _, voteReceivedNode := range txnVoteReceivedNodes {
				startingNode.AddEdgeTo(voteReceivedNode)
			}

			if majorityVoting {
				for _, startingNode2 := range txnStartingNodes {
					if startingNode != startingNode2 {
						startingNode.AddEdgeTo(startingNode2)
					}
				}
				startingNode.Callback = startingCallback
			}
		}

		for _, voteReceivedNode := range txnVoteReceivedNodes {
			for _, completionReceivedNode := range txnCompletionReceivedNodes {
				voteReceivedNode.AddEdgeTo(completionReceivedNode)
			}
			voteReceivedNode.Callback = votingCallback
		}

		if needsCompletionNodes {
			completionCallback := gsim.NewAvailableAllCallback(txnVoteReceivedNodes...)
			for _, completionReceivedNode := range txnCompletionReceivedNodes {
				completionReceivedNode.Callback = completionCallback
			}
		}

		startingNodes = append(startingNodes, txnStartingNodes...)
	}

	if snm != nil {
		startingNodes = snm(startingNodes, startingNodesMap)
	}

	return gsim.NewGraphPermutation(startingNodes...), &consumer{
		varToVIs:       varToVIs,
		txnToVIs:       txnToVIs,
		majorityVoting: majorityVoting,
	}
}

type majorityCallback struct {
	result      gsim.GraphNodeStateChange
	majRequired map[*gsim.GraphNode]bool
	majDec      int // this is majority minus 1 (i.e. F)
}

func NewAvailableMajorityCallback(majRequired ...*gsim.GraphNode) *majorityCallback {
	return newMajorityCallback(gsim.MakeAvailable, majRequired...)
}

func NewInhibitMajorityCallback(majRequired ...*gsim.GraphNode) *majorityCallback {
	return newMajorityCallback(gsim.Inhibit, majRequired...)
}

func newMajorityCallback(result gsim.GraphNodeStateChange, majRequired ...*gsim.GraphNode) *majorityCallback {
	twoFInc := len(majRequired)
	majDec := twoFInc >> 1
	majMap := make(map[*gsim.GraphNode]bool, len(majRequired))
	for _, node := range majRequired {
		majMap[node] = true
	}
	return &majorityCallback{
		result:      result,
		majRequired: majMap,
		majDec:      majDec,
	}
}

func (mc *majorityCallback) IncomingEdgesReached(node *gsim.GraphNode, reached []*gsim.GraphNode) gsim.GraphNodeStateChange {
	if len(reached) > mc.majDec {
		count := 0
		for _, reachedNode := range reached {
			if _, found := mc.majRequired[reachedNode]; found {
				count++
				if count > mc.majDec {
					return mc.result
				}
			}
		}
	}
	return gsim.NoChange
}

// VarInstance

type VarInstance struct {
	p.Var
	id string
}

func NewVarInstance(id string, v p.Var) *VarInstance {
	return &VarInstance{
		Var: v,
		id:  id,
	}
}

func (vi *VarInstance) String() string {
	return fmt.Sprintf("VI%v(%v)", vi.id, vi.Var.String())
}

// VarInstances

type VarInstances []*VarInstance

func (vi VarInstances) Len() int           { return len(vi) }
func (vi VarInstances) Less(i, j int) bool { return vi[i].id < vi[j].id }
func (vi VarInstances) Swap(i, j int)      { vi[i], vi[j] = vi[j], vi[i] }
func (vi VarInstances) Sort()              { sort.Sort(vi) }

// votesNotReady

var (
	vnr = fmt.Errorf("Votes Not Ready")
	cnr = fmt.Errorf("Completion Not Ready")
)

// consumer

type consumer struct {
	majorityVoting  bool
	serialHistories [][]*p.HistoryNode
	varToVIs        map[p.Var]*VarInstances
	txnToVIs        map[*p.Txn]VarInstances
	varInstances    VarInstances
	engine          TxnEngine
	histogram       *Histogram
}

func (c *consumer) Clone() gsim.PermutationConsumer {
	d := &consumer{
		majorityVoting:  c.majorityVoting,
		serialHistories: c.serialHistories,
		varToVIs:        make(map[p.Var]*VarInstances),
		txnToVIs:        make(map[*p.Txn]VarInstances),
		varInstances:    c.varInstances,
		engine:          c.engine.Clone(),
		histogram:       c.histogram.Clone(),
	}
	for k, v := range c.varToVIs {
		d.varToVIs[k] = v
	}
	for k, v := range c.txnToVIs {
		d.txnToVIs[k] = v
	}
	return d
}

func (c *consumer) Consume(n *big.Int, perm []interface{}) {
	if len(perm) == 0 {
		return
	}
	// For scenario generation only:
	//c.histogram.Add(0, 0, false, perm[0].(*gsim.GraphNode).Value.(txnInstruction))
	//return
	instrs := make([]txnInstruction, len(perm))
	for idx, instr := range perm {
		instrs[idx] = instr.(*gsim.GraphNode).Value.(txnInstruction)
	}
	// fmt.Println(n, len(instrs), instrs)
	state := c.generateFreshState()
	fatal := func(instr txnInstruction, err error) {
		log.Printf("\nFatal Error (%v) at instruction %v\nInstructions:\n%v\n", n, instr, instrs)
		for _, vi := range c.varInstances {
			log.Printf("History for %v:\n%v\n", vi, state.viState[vi].CommitHistory())
		}
		log.Panic(err)
	}
	cnrEncountered := false
	for idx, instr := range instrs {
		err := state.interpret(instr)
		if cnrEncountered && err != cnr {
			return
		} else if err == vnr {
			foundReceiveTxn := false
			anyVoteAvailable := false
			for _, remainingInstr := range instrs[idx+1:] {
				if _, ok := remainingInstr.(*txnReceived); ok {
					foundReceiveTxn = true
				} else if vote, ok := remainingInstr.(*txnVotesReceived); ok {
					anyVoteAvailable = anyVoteAvailable || state.ballots[vote.txn].AllVotesReceived()
				}
				if foundReceiveTxn || anyVoteAvailable {
					break
				}
			}
			if !foundReceiveTxn && !anyVoteAvailable {
				fatal(instr, fmt.Errorf("All txns have been received but no votes are ready. Deadlock."))
			}
			return
		} else if err == cnr {
			cnrEncountered = true
		} else if err != nil {
			fatal(instr, err)
			return
		}
	}

	commitCount, abortCount, isPrefix, err := c.verifyHistoryIsSerial(state)
	if err != nil {
		fatal(nil, err)
	}
	c.histogram.Add(commitCount, abortCount, isPrefix, instrs[0])
}

func (c *consumer) generateFreshState() *state {
	state := &state{
		ballots: make(map[*p.Txn]Ballot),
		viState: make(map[*VarInstance]*viState),
	}

	for txn, vis := range c.txnToVIs {
		votesRequired := len(vis)
		if c.majorityVoting {
			votesRequired = (votesRequired >> 1) + 1
		}
		state.ballots[txn] = c.engine.NewBallot(txn, votesRequired, len(vis))
	}

	for _, vi := range c.varInstances {
		varState := &p.VarVersionValue{
			Var:     vi.Var,
			Version: 0,
			Value:   nil,
		}
		state.viState[vi] = &viState{EngineVar: c.engine.NewEngineVar(vi, varState)}
	}

	return state
}

// state

type state struct {
	ballots map[*p.Txn]Ballot
	viState map[*VarInstance]*viState
}

func (s *state) interpret(instr txnInstruction) error {
	switch it := instr.(type) {
	case *txnReceived:
		return s.viState[it.vi].txnReceived(it.txn, s.ballots[it.txn])
	case *txnVotesReceived:
		return s.viState[it.vi].txnVotesReceived(it.txn, s.ballots[it.txn])
	case *txnGloballyCompleteReceived:
		return s.viState[it.vi].txnGloballyCompleteReceived(it.txn, s.ballots[it.txn])
	default:
		return fmt.Errorf("Unknown instruction %v\n", instr)
	}
	return nil
}

// viState

type viState struct{ EngineVar }

func (vis *viState) txnReceived(txn *p.Txn, ballot Ballot) error {
	return vis.TxnReceived(txn, ballot)
}

func (vis *viState) txnVotesReceived(txn *p.Txn, ballot Ballot) error {
	if ballot.AllVotesReceived() {
		return vis.TxnVotesReceived(txn, ballot)
	} else {
		return vnr
	}
}

func (vis *viState) txnGloballyCompleteReceived(txn *p.Txn, ballot Ballot) error {
	if ballot.AllLocallyComplete() {
		return vis.TxnGloballyCompleteReceived(txn, ballot)
	} else {
		return cnr
	}
}

// Ballot

type Ballot interface {
	IsAbort() bool
	IsStable() bool
	AllVotesReceived() bool
	AllLocallyComplete() bool
}

// TxnEngine

type TxnEngine interface {
	NewBallot(*p.Txn, int, int) Ballot
	NewEngineVar(*VarInstance, *p.VarVersionValue) EngineVar
	NeedsCompletionNodes() bool
	Clone() TxnEngine
}

// EngineVar

type EngineVar interface {
	TxnReceived(*p.Txn, Ballot) error
	TxnVotesReceived(*p.Txn, Ballot) error
	TxnGloballyCompleteReceived(*p.Txn, Ballot) error
	CommitHistory() *p.HistoryNode
}

// No completion supertypes

type NoCompletionTxnEngine struct{}

func (ncte *NoCompletionTxnEngine) NeedsCompletionNodes() bool {
	return false
}

type NoCompletionTxnEngineVar struct{}

func (nctev *NoCompletionTxnEngineVar) TxnGloballyCompleteReceived(txn *p.Txn, ballot Ballot) error {
	return nil
}

type NoCompletionBallot struct{}

func (ncb *NoCompletionBallot) AllLocallyComplete() bool {
	log.Panic("Unreachable code: NoCompletionBallot used with TxnEngine that requires Completion")
	return false
}

// txn instructions

type txnInstruction interface {
	txnInstructionWitness()
}

type txnReceived struct {
	txn *p.Txn
	vi  *VarInstance
}

func (tr *txnReceived) txnInstructionWitness() {}

func (tr *txnReceived) String() string {
	return fmt.Sprintf("Txn Received by %v: %v", tr.vi, tr.txn)
}

type txnVotesReceived struct {
	txn *p.Txn
	vi  *VarInstance
}

func (tvr *txnVotesReceived) txnInstructionWitness() {}

func (tvr *txnVotesReceived) String() string {
	return fmt.Sprintf("Txn Votes Received by %v: %v", tvr.vi, tvr.txn)
}

type txnGloballyCompleteReceived struct {
	txn *p.Txn
	vi  *VarInstance
}

func (tgcr *txnGloballyCompleteReceived) txnInstructionWitness() {}

func (tgcr *txnGloballyCompleteReceived) String() string {
	return fmt.Sprintf("Txn Globally Complete Received by %v: %v", tgcr.vi, tgcr.txn)
}
