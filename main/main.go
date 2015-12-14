package main

import (
	"flag"
	"fmt"
	"github.com/msackman/gsim"
	p "goshawkdb.io/simulation"
	"goshawkdb.io/simulation/engines"
	"goshawkdb.io/simulation/parallel"
	"goshawkdb.io/simulation/serial"
	"log"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println(os.Args)

	runtime.GOMAXPROCS(runtime.NumCPU())

	var cpuprofile string
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	var profileFile *os.File
	if cpuprofile != "" {
		var err error
		profileFile, err = os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
	}

	go signalHandler(profileFile)

	/*
		1,2,3
		1,3,2
		2,1,3
		2,3,1
		3,1,2
		3,2,1
	*/
	/*
		// r0r3, r0w->2, w->3 cycle
		// 2015/07/23 16:13:44.090851 5184 scenarios found.
		ThreeThreeScenarios(0, nil, func(txn *p.Txn) bool {
			if txn.ID == 3 {
				// double read
				if len(txn.Actions) != 2 {
					return false
				}
				for _, action := range txn.Actions {
					if action.IsWrite() {
						return false
					}
				}
				return true

			} else if txn.ID == 2 {
				// read write
				if len(txn.Actions) != 2 {
					return false
				}
				rok := false
				wok := false
				for _, action := range txn.Actions {
					if action.IsRead() {
						rok = true
					} else {
						wok = true
					}
				}
				return rok && wok

			} else if txn.ID == 1 {
				// single write
				if len(txn.Actions) != 1 {
					return false
				}
				return txn.Actions[0].IsWrite()
			}
			return false
		})
	*/
	/*
		// pure rw-cycle
		readVars := []p.Var{"_", "x", "y", "z"}
		writeVars := []p.Var{"_", "y", "z", "x"}
		ThreeThreeScenarios(1, bigIntMinus1, func(txn *p.Txn) bool {
			if len(txn.Actions) != 2 {
				return false
			}
			rok := false
			wok := false
			for _, action := range txn.Actions {
				if action.IsRead() {
					if action.ReadVersion == 0 && readVars[txn.ID] == action.Var {
						rok = true
					} else {
						return false
					}
				} else {
					if writeVars[txn.ID] == action.Var {
						wok = true
					} else {
						return false
					}
				}
			}
			return rok && wok
		})
	*/
	/*
		// 3 txns, at least 2 with reads
		reads := []int{0, 0, 0, 0}
		ThreeThreeScenarios(0, nil, func(txn *p.Txn) bool {
			reads[txn.ID] = 0
			if len(txn.Actions) == 0 {
				return false
			}
			for _, action := range txn.Actions {
				if action.IsRead() {
					reads[txn.ID] = 1
					break
				}
			}
			if txn.ID == 1 {
				// 1 is the last one to be generated
				return (reads[3] + reads[2] + reads[1]) > 1
			} else if txn.ID == 2 {
				return (reads[3] + reads[2]) > 0
			} else {
				return true
			}
		})
	*/
	/*
		// 3 txns, at least 2 reads, 2 writes in total
		reads := []int{0, 0, 0, 0}
		writes := []int{0, 0, 0, 0}
		TwoThreeScenarios(0, nil, func(txn *p.Txn) bool {
			reads[txn.ID] = 0
			writes[txn.ID] = 0
			if len(txn.Actions) == 0 {
				return false
			}
			for _, action := range txn.Actions {
				if action.IsRead() {
					reads[txn.ID] = 1
				}
				if action.IsWrite() {
					writes[txn.ID] = 1
				}
			}
			if txn.ID == 1 {
				// 1 is the last one to be generated
				return ((reads[3] + reads[2] + reads[1]) > 1) && ((writes[3] + writes[2] + writes[1]) > 1)
			} else if txn.ID == 2 {
				return ((reads[3] + reads[2]) > 0) && ((writes[3] + writes[2]) > 0)
			} else {
				return true
			}
		})
	*/
	/*
		// 3 vars, 3 txns, 2 vars per txn, all writes
		ThreeThreeScenarios(0, nil, func(txn *p.Txn) bool {
			if len(txn.Actions) != 2 {
				return false
			}
			for _, action := range txn.Actions {
				if action.IsRead() {
					return false
				}
			}
			return true
		})
	*/
	/*
		// 2 vars, 4 txns, all writes
		TwoFourScenarios(0, nil, func(txn *p.Txn) bool {
			if len(txn.Actions) == 0 {
				return false
			}
			for _, action := range txn.Actions {
				if action.IsRead() {
					return false
				}
			}
			return true
		})
	*/
	//TwoFourScenarios(0, nil, func(txn *p.Txn) bool { return len(txn.Actions) != 0 })
	// Problem with DupVI is that scenario 111 (all w[x], and onwards
	// eg 112 w[x],w[x],r[x0]) has around 43bn permutations - costs
	// about 2 days just to *generate* them all.
	//TwoDupVIScenarios()
	//ThreeFourScenarios(0, nil, func(txn *p.Txn) bool { return len(txn.Actions) != 0 })
	//ThreeThreeScenarios(0, nil, nil)
	TwoThreeScenarios(0, nil, nil)
	//TwoTwoScenarios(0, nil, nil)
	//TwoOneScenarios(0, nil, nil)

	//TwoThreeMixedRead()
	//TwoThree()
	//ThreeThree1R1WScenario()
	//SpecialScenario()
	//Custom()
}

func Custom() {
	viB := parallel.NewVarInstance("B", "x")
	viC := parallel.NewVarInstance("C", "y")
	vis := []*parallel.VarInstance{viB, viC}
	txn1 := p.NewTxn(1, p.WriteAction("x", "Eggs1"), p.WriteAction("y", "Why1"))
	txn2 := p.NewTxn(2, p.WriteAction("x", "Eggs2"))
	txn3 := p.NewTxn(3, p.WriteAction("x", "Eggs3"), p.WriteAction("y", "Why3"))
	txn4 := p.NewTxn(4, p.WriteAction("y", "Why4"))
	txns := []*p.Txn{txn1, txn2, txn3, txn4}
	RunScenario(txns, vis, nil, nil, false)
}

func SpecialScenario() {
	viB := parallel.NewVarInstance("B", "x")
	viC := parallel.NewVarInstance("C", "y")
	viD := parallel.NewVarInstance("D", "z")
	vis := []*parallel.VarInstance{viB, viC, viD}

	txn1 := p.NewTxn(1, p.ReadAction("x", 4), p.WriteAction("z", "Zed1"))
	txn2 := p.NewTxn(2, p.WriteAction("y", "Why2"), p.WriteAction("z", "Zed2"))
	txn3 := p.NewTxn(3, p.ReadAction("x", 0), p.WriteAction("y", "Why3"))
	txn4 := p.NewTxn(4, p.WriteAction("x", "Eggs4"))
	txns := []*p.Txn{txn1, txn2, txn3, txn4}

	snm := func(startingNodes []*gsim.GraphNode, nodeMap map[*p.Txn]map[*parallel.VarInstance][]*gsim.GraphNode) []*gsim.GraphNode {
		txn3x := nodeMap[txn3][viB][0]
		txn4x := nodeMap[txn4][viB][0]
		txn3x.AddEdgeTo(txn4x)
		txn4x.Callback = gsim.NewAvailableAllCallback(txn3x)

		txn4bx := nodeMap[txn4][viB][1]
		txn1x := nodeMap[txn1][viB][0]
		txn4bx.AddEdgeTo(txn1x)
		txn1x.Callback = gsim.NewAvailableAllCallback(txn4bx)

		txn2y := nodeMap[txn2][viC][0]
		//txn1x.AddEdgeTo(txn2y)
		//txn2y.Callback = gsim.NewAvailableAllCallback(txn1x)

		txn3y := nodeMap[txn3][viC][0]
		txn2y.AddEdgeTo(txn3y)
		txn3y.Callback = gsim.NewAvailableAllCallback(txn2y)

		txn1z := nodeMap[txn1][viD][0]
		//txn3y.AddEdgeTo(txn1z)
		//txn1z.Callback = gsim.NewAvailableAllCallback(txn3y)

		txn2z := nodeMap[txn2][viD][0]
		txn1z.AddEdgeTo(txn2z)
		txn2z.Callback = gsim.NewAvailableAllCallback(txn1z)

		//startingNodes[0] = txn3x
		//return startingNodes[:1]
		return []*gsim.GraphNode{txn3x, txn2y, txn1z}
	}
	RunScenario(txns, vis, nil, snm, false)
}

func TwoThree() {
	vis := []*parallel.VarInstance{
		parallel.NewVarInstance("B", "x"),
		parallel.NewVarInstance("C", "y"),
	}

	// all writes
	log.Println("Two by Three: All writes (lowest double) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1"), p.WriteAction("y", "Why1")),
		p.NewTxn(2, p.WriteAction("x", "Eggs2")),
		p.NewTxn(3, p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)
	log.Println("Two by Three: All writes (middle double) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1")),
		p.NewTxn(2, p.WriteAction("x", "Eggs2"), p.WriteAction("y", "Why2")),
		p.NewTxn(3, p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)
	log.Println("Two by Three: All writes (upper double) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1")),
		p.NewTxn(2, p.WriteAction("y", "Why2")),
		p.NewTxn(3, p.WriteAction("x", "Eggs3"), p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)

	log.Println("Two by Three: All writes (lowest single) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1")),
		p.NewTxn(2, p.WriteAction("x", "Eggs2"), p.WriteAction("y", "Why2")),
		p.NewTxn(3, p.WriteAction("x", "Eggs3"), p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)
	log.Println("Two by Three: All writes (middle single) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1"), p.WriteAction("y", "Why1")),
		p.NewTxn(2, p.WriteAction("x", "Eggs2")),
		p.NewTxn(3, p.WriteAction("x", "Eggs3"), p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)
	log.Println("Two by Three: All writes (upper single) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1"), p.WriteAction("y", "Why1")),
		p.NewTxn(2, p.WriteAction("x", "Eggs2"), p.WriteAction("y", "Why2")),
		p.NewTxn(3, p.WriteAction("x", "Eggs3")),
	}, vis, nil, nil, false)

	log.Println("Two by Three: All writes (all double) (all can commit)")
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1"), p.WriteAction("y", "Why1")),
		p.NewTxn(2, p.WriteAction("x", "Eggs2"), p.WriteAction("y", "Why2")),
		p.NewTxn(3, p.WriteAction("x", "Eggs3"), p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)
}

func TwoThreeMixedRead() {
	vis := []*parallel.VarInstance{
		parallel.NewVarInstance("B", "x"),
		parallel.NewVarInstance("C", "y"),
	}
	RunScenario([]*p.Txn{
		p.NewTxn(1, p.WriteAction("x", "Eggs1"), p.WriteAction("y", "Why1")),
		p.NewTxn(2, p.ReadAction("x", 3)),
		p.NewTxn(3, p.WriteAction("x", "Eggs3"), p.WriteAction("y", "Why3")),
	}, vis, nil, nil, false)
}

func ThreeThree1R1WScenario() {
	txns := []*p.Txn{
		p.NewTxn(1, p.ReadAction("x", 0), p.WriteAction("y", "Why")),
		p.NewTxn(2, p.ReadAction("y", 0), p.WriteAction("z", "Zed")),
		p.NewTxn(3, p.ReadAction("z", 0), p.WriteAction("x", "Eggs")),
	}
	vis := []*parallel.VarInstance{
		parallel.NewVarInstance("B", "x"),
		parallel.NewVarInstance("C", "y"),
		parallel.NewVarInstance("D", "z"),
	}
	RunScenario(txns, vis, nil, nil, false)
}

type ScenarioConsumer func(int, p.Txns)
type TxnFilter func(*p.Txn) bool

func nilFilter(txn *p.Txn) bool {
	return true
}

func CreateScenarios(vars []p.Var, varValue map[p.Var]interface{}, txnCount int, filter TxnFilter, consumer ScenarioConsumer) {
	if filter == nil {
		filter = nilFilter
	}
	readVersions := make([]int, txnCount+1)
	for idx := range readVersions {
		readVersions[idx] = idx
	}
	// readActions := [][]*p.VarAction{[]*p.VarAction{}}
	readActions := createReadActions(vars, readVersions)
	writeActions := createWriteActions(vars, varValue)

	txnFuncs := []func(int) *p.Txn{}
	for _, read := range readActions {
		readCopy := make([]*p.VarAction, len(read))
		copy(readCopy, read)
		for _, write := range writeActions {
			writeCopy := write
			txnFuncs = append(txnFuncs, func(txnID int) *p.Txn {
				return p.NewTxn(txnID, append(writeCopy(txnID), readCopy...)...)
			})
		}
	}

	completedScenarioCount := generateTxns(filter, txnFuncs, consumer, make([]*p.Txn, txnCount), 0, txnCount, 0)
	log.Println(completedScenarioCount, "scenarios found.")
}

func generateTxns(
	filter TxnFilter,
	txnFuncs []func(int) *p.Txn,
	consumer ScenarioConsumer,
	txnsAcc p.Txns,
	txnsAccIdx int,
	txnCount int,
	scenarioCount int) int {

	if txnCount == 0 {
		if txnsAccIdx != 0 {
			scenarioCount++
			consumer(scenarioCount, txnsAcc[:txnsAccIdx])
		}
		return scenarioCount

	} else {
		txnCount1 := txnCount - 1
		txnsAccIdx1 := txnsAccIdx
		for _, f := range txnFuncs {
			txn := f(txnCount)
			if !filter(txn) {
				continue
			}
			if len(txn.Actions) == 0 {
				txnsAccIdx1 = txnsAccIdx
			} else {
				txnsAcc[txnsAccIdx] = txn
				txnsAccIdx1 = txnsAccIdx + 1
			}
			scenarioCount = generateTxns(filter, txnFuncs, consumer, txnsAcc, txnsAccIdx1, txnCount1, scenarioCount)
		}
		return scenarioCount
	}
}

func createReadActions(vars []p.Var, versions []int) [][]*p.VarAction {
	result := [][]*p.VarAction{[]*p.VarAction{}}
	readActions := make([]interface{}, len(vars))

	for idx, v := range vars {
		reads := make([]interface{}, len(versions))
		for idy, version := range versions {
			reads[idy] = p.ReadAction(v, version)
		}
		readActions[idx] = reads
	}
	for _, comb := range combinations(readActions) {
		product := make([][]interface{}, len(comb))
		for idx, varActionsProduct := range comb {
			product[idx] = varActionsProduct.([]interface{})
		}
		for _, e := range cartesianProduct(appendCombinator, product) {
			eList := e.([]interface{})
			actions := make([]*p.VarAction, len(eList))
			for idx, eListElem := range eList {
				actions[idx] = eListElem.(*p.VarAction)
			}
			result = append(result, actions)
		}
	}

	return result
}

func createWriteActions(vars []p.Var, varValue map[p.Var]interface{}) []func(int) []*p.VarAction {
	result := []func(int) []*p.VarAction{
		func(v int) []*p.VarAction { return []*p.VarAction{} },
	}

	varsIface := make([]interface{}, len(varValue))
	for idx, v := range vars {
		varsIface[idx] = v
	}

	for _, comb := range combinations(varsIface) {
		combVars := make([]p.Var, len(comb))
		for idx, v := range comb {
			combVars[idx] = v.(p.Var)
		}
		result = append(result, func(txnID int) []*p.VarAction {
			writeActions := make([]*p.VarAction, len(combVars))
			for idx, v := range combVars {
				writeActions[idx] = p.WriteAction(v, fmt.Sprintf("%v%v", varValue[v], txnID))
			}
			return writeActions
		})
	}

	return result
}

func appendCombinator(head interface{}, tail interface{}) interface{} {
	tailList := tail.([]interface{})
	result := make([]interface{}, 1+len(tailList))
	result[0] = head
	copy(result[1:], tailList)
	return result
}

func cartesianProduct(combinator func(interface{}, interface{}) interface{}, elems [][]interface{}) []interface{} {
	if len(elems) == 0 {
		return []interface{}{}
	}
	elem := elems[0]
	if len(elems) == 1 {
		result := make([]interface{}, len(elem))
		for idx, e := range elem {
			result[idx] = []interface{}{e}
		}
		return result
	}
	result := []interface{}{}
	for _, tail := range cartesianProduct(combinator, elems[1:]) {
		for _, head := range elem {
			result = append(result, combinator(head, tail))
		}
	}
	return result
}

func combinations(elems []interface{}) [][]interface{} {
	result := [][]interface{}{}
	for k := 1; k <= len(elems); k++ {
		result = append(result, combinationsK(elems, k)...)
	}
	return result
}

func combinationsK(elems []interface{}, k int) [][]interface{} {
	if k > len(elems) || k <= 0 {
		return [][]interface{}{}
	}
	if k == len(elems) {
		return [][]interface{}{elems}
	}
	if k == 1 {
		result := make([][]interface{}, len(elems))
		for idx, elem := range elems {
			result[idx] = []interface{}{elem}
		}
		return result
	}
	result := [][]interface{}{}
	for idx := 0; idx < len(elems)-k+1; idx++ {
		for _, tailComb := range combinationsK(elems[idx+1:], k-1) {
			comb := make([]interface{}, len(tailComb)+1)
			comb[0] = elems[idx]
			copy(comb[1:], tailComb)
			result = append(result, comb)
		}
	}
	return result
}

func TwoOneScenarios(sNum int, permNum *big.Int, filter TxnFilter) {
	TwoXScenarios(1, sNum, permNum, filter)
}
func TwoTwoScenarios(sNum int, permNum *big.Int, filter TxnFilter) {
	TwoXScenarios(2, sNum, permNum, filter)
}
func TwoThreeScenarios(sNum int, permNum *big.Int, filter TxnFilter) {
	TwoXScenarios(3, sNum, permNum, filter)
}
func TwoFourScenarios(sNum int, permNum *big.Int, filter TxnFilter) {
	TwoXScenarios(4, sNum, permNum, filter)
}

func TwoDupVIScenarios() {
	viB := parallel.NewVarInstance("B", "x")
	viC := parallel.NewVarInstance("C", "x")
	viD := parallel.NewVarInstance("D", "x")
	vis := []*parallel.VarInstance{viB, viC, viD}

	writeVals := make(map[p.Var]interface{})
	writeVals["x"] = "Eggs"
	CreateScenarios([]p.Var{"x"}, writeVals, 3, nil, scenarioConsumer(vis, 0, nil, nil, true))
}

func TwoXScenarios(txnCount int, sNum int, permNum *big.Int, filter TxnFilter) {
	vis := []*parallel.VarInstance{
		parallel.NewVarInstance("B", "x"),
		parallel.NewVarInstance("C", "y"),
	}

	writeVals := make(map[p.Var]interface{})
	writeVals["x"] = "Eggs"
	writeVals["y"] = "Why"

	CreateScenarios([]p.Var{"x", "y"}, writeVals, txnCount, filter, scenarioConsumer(vis, sNum, permNum, nil, false))
}

func ThreeThreeScenarios(sNum int, permNum *big.Int, filter TxnFilter) {
	ThreeXScenarios(3, sNum, permNum, filter)
}
func ThreeFourScenarios(sNum int, permNum *big.Int, filter TxnFilter) {
	ThreeXScenarios(4, sNum, permNum, filter)
}

func ThreeXScenarios(txnCount int, sNum int, permNum *big.Int, filter TxnFilter) {
	vis := []*parallel.VarInstance{
		parallel.NewVarInstance("B", "x"),
		parallel.NewVarInstance("C", "y"),
		parallel.NewVarInstance("D", "z"),
	}

	writeVals := make(map[p.Var]interface{})
	writeVals["x"] = "Eggs"
	writeVals["y"] = "Why"
	writeVals["z"] = "Zed"

	CreateScenarios([]p.Var{"x", "y", "z"}, writeVals, txnCount, filter, scenarioConsumer(vis, sNum, permNum, nil, false))
}

var (
	bigIntMinus1 = new(big.Int).SetInt64(-1)
)

func scenarioConsumer(vis []*parallel.VarInstance, sNum int, permNum *big.Int, snm parallel.StartingNodeModifier, majorityVoting bool) ScenarioConsumer {
	return func(n int, scenario p.Txns) {
		if permNum == nil {
			if n < sNum {
				return
			}
			log.Printf("Scenario %v:\n\t%v", n, scenario)
			RunScenario(scenario, vis, nil, snm, majorityVoting)

		} else {
			if n != sNum {
				return
			}
			log.Printf("Scenario %v:\n\t%v", n, scenario)
			if bigIntMinus1.Cmp(permNum) == 0 {
				RunScenario(scenario, vis, nil, snm, majorityVoting)
			} else {
				RunScenario(scenario, vis, permNum, snm, majorityVoting)
			}
		}
	}
}

func RunScenario(txnsOrig []*p.Txn, vis []*parallel.VarInstance, n *big.Int, snm parallel.StartingNodeModifier, majorityVoting bool) {
	// due to parallelism, need to ensure every scenario is in its own slice.
	txns := make([]*p.Txn, len(txnsOrig))
	copy(txns, txnsOrig)
	successfulTxns := make(map[*p.Txn]bool)
	serialHistories := serial.FindAllHistories(txns...)
	for _, history := range serialHistories {
		if history == nil {
			continue // no txns were committable
		}
		node := history
		for {
			successfulTxns[node.CommittedTxn] = true
			if len(node.Next) == 0 {
				break
			} else {
				node = node.Next[0]
			}
		}
	}

	if len(successfulTxns) < len(txns) {
		fmt.Println("Scenario contains uncommittable txns:")
		for _, txn := range txns {
			if !successfulTxns[txn] {
				fmt.Println(txn)
			}
		}
		fmt.Println()
		return
	}

	fmt.Println("Serial Histories:")
	strs := make([]string, len(serialHistories))
	for idx, history := range serialHistories {
		strs[idx] = history.String()
	}
	fmt.Printf("--\n%v--\n", strings.Join(strs, "-\n"))

	var histogram *parallel.Histogram
	if n == nil {
		histogram = parallel.ValidateAllHistories(engines.NewMatthew7TxnEngine(vis), txns, vis, snm, majorityVoting, serialHistories)
	} else {
		histogram = parallel.ValidateHistory(engines.NewMatthew7TxnEngine(vis), txns, vis, snm, majorityVoting, serialHistories, n)
	}
	fmt.Println(histogram)
}

func signalHandler(profileFile *os.File) {
	profilerRunning := false
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGUSR1, os.Interrupt)
	for {
		sig := <-sigs
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			if profilerRunning {
				pprof.StopCPUProfile()
				profilerRunning = false
			}
			os.Exit(0)
		case syscall.SIGUSR1:
			if profilerRunning {
				pprof.StopCPUProfile()
				profilerRunning = false
				log.Println("Profiler stopped")
			} else if profileFile != nil {
				pprof.StartCPUProfile(profileFile)
				profilerRunning = true
				log.Println("Profiler started")
			}
		}
	}
}
