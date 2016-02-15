package parallel

import (
	"fmt"
	sl "github.com/msackman/skiplist"
	"log"
	"math/rand"
	"strings"
)

type Histogram struct {
	cloneChan     chan []*Histogram
	totalCount    int
	prefixCount   int
	commitHist    *sl.SkipList
	abortHist     *sl.SkipList
	initialInstrs []txnInstruction
	rng           *rand.Rand
}

func NewHistogram(cc chan []*Histogram, rng *rand.Rand) *Histogram {
	result := &Histogram{
		cloneChan:     cc,
		totalCount:    0,
		prefixCount:   0,
		commitHist:    sl.New(rng),
		abortHist:     sl.New(rng),
		initialInstrs: []txnInstruction{},
		rng:           rng,
	}
	hists := []*Histogram{result}
	for {
		select {
		case result.cloneChan <- hists:
			return result
		case existing := <-result.cloneChan:
			hists = append(hists, existing...)
		}
	}
}

func (h *Histogram) Clone() *Histogram {
	return NewHistogram(h.cloneChan, rand.New(rand.NewSource(0)))
}

func (h *Histogram) GatherAll() *Histogram {
	rng := rand.New(rand.NewSource(0))
	result := &Histogram{
		totalCount:  0,
		prefixCount: 0,
		commitHist:  sl.New(rng),
		abortHist:   sl.New(rng),
	}
	close(h.cloneChan)
	for hList := range h.cloneChan {
		for _, h := range hList {
			result.totalCount += h.totalCount
			result.prefixCount += h.prefixCount

			for newElem := h.commitHist.First(); newElem != nil; newElem = newElem.Next() {
				oldElem := result.commitHist.Get(newElem.Key)
				if oldElem == nil {
					result.commitHist.Insert(newElem.Key, newElem.Value)
				} else {
					oldElem.Value = oldElem.Value.(int) + newElem.Value.(int)
				}
			}

			for newElem := h.abortHist.First(); newElem != nil; newElem = newElem.Next() {
				oldElem := result.abortHist.Get(newElem.Key)
				if oldElem == nil {
					result.abortHist.Insert(newElem.Key, newElem.Value)
				} else {
					oldElem.Value = oldElem.Value.(int) + newElem.Value.(int)
				}
			}
		}
	}
	return result
}

func (h *Histogram) Add(commitCount, abortCount int, isPrefix bool, instr txnInstruction) {
	h.totalCount++
	if isPrefix {
		h.prefixCount++
	}

	commitElem := h.commitHist.Get(intKey(commitCount))
	if commitElem == nil {
		h.commitHist.Insert(intKey(commitCount), 1)
	} else {
		commitElem.Value = commitElem.Value.(int) + 1
	}

	abortElem := h.abortHist.Get(intKey(abortCount))
	if abortElem == nil {
		h.abortHist.Insert(intKey(abortCount), 1)
	} else {
		abortElem.Value = abortElem.Value.(int) + 1
	}

	if l := len(h.initialInstrs); l == 0 || instr != h.initialInstrs[l-1] {
		h.initialInstrs = append(h.initialInstrs, instr)
	}

	if h.totalCount%100000 == 0 {
		log.Println(h)
	}
}

func (h *Histogram) String() string {
	commitHistStr := []string{}
	for elem := h.commitHist.First(); elem != nil; elem = elem.Next() {
		commitHistStr = append(commitHistStr, fmt.Sprintf("%v\t: %v", elem.Key, elem.Value))
	}
	abortHistStr := []string{}
	for elem := h.abortHist.First(); elem != nil; elem = elem.Next() {
		abortHistStr = append(abortHistStr, fmt.Sprintf("%v\t: %v", elem.Key, elem.Value))
	}
	return fmt.Sprintf(
		"Total:\t%v\nPrefix:\t%v (%v%%)\nCommit Histogram:\n%v\nAbort Histogram:\n%v\nInitial Instructions:\n%v\n",
		h.totalCount, h.prefixCount, float32(h.prefixCount*100)/float32(h.totalCount),
		strings.Join(commitHistStr, "\n"),
		strings.Join(abortHistStr, "\n"),
		h.initialInstrs,
	)
}

type intKey int

func (a intKey) Compare(bC sl.Comparable) sl.Cmp {
	b := bC.(intKey)
	switch {
	case a < b:
		return sl.LT
	case a > b:
		return sl.GT
	default:
		return sl.EQ
	}
}
