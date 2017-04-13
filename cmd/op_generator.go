package cmd

import (
	"time"
	"math/rand"
	"fmt"
	"os"
	"strconv"
	"sort"
)

var (
	prefix = "/moon/etcd-sim-"
	write_index = 1
	write_limit = 10000
	r *rand.Rand
)

type GetOp func ()*Op

type generator struct {
	*timeChecker
	op GetOp
	s *sender
}

type timeChecker struct {
	interval time.Duration
	exceptTime time.Time
}

func newGenerator(qps uint, op GetOp) *generator {
	g := generator{}
	g.timeChecker = &timeChecker{}
	g.timeChecker.interval = time.Second/time.Duration(qps)
	g.op = op
	return &g
}

func (g * generator) start(benchMode bool) {
	if benchMode {
		g.interval = g.interval/100000
	}
	g.exceptTime = time.Now()
}

func (t *timeChecker) checkAndUpdate(now time.Time) bool {
	if now.Before(t.exceptTime) {
		return false
	}
	t.exceptTime = t.exceptTime.Add(t.interval)
	return true
}

func getOp() *Op {
	i := r.Intn(write_index)
	key := prefix+strconv.Itoa(i)
	get := Get{key:key}
	return &Op{get:&get}
}

func listOp() *Op {
	r := List {
		prefix: prefix,
	}
	return &Op{list:&r}
}

func mustRandBytes(n int) []byte {
	rb := make([]byte, n)
	_, err := rand.Read(rb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate value: %v\n", err)
		os.Exit(1)
	}
	return rb
}

func updateOp() *Op {
	i := r.Intn(write_index)
	key := prefix+strconv.Itoa(i)
	value := mustRandBytes(r.Intn(4096))
	u := Update{key:key,value: string(value)}
	return &Op{update:&u}
}

func createOp() *Op {
	write_index = write_index + 1
	key := prefix + strconv.Itoa(write_index)
	value := mustRandBytes(r.Intn(4096))
	p := Put{key:key,value: string(value)}
	return &Op{put:&p}
}

type OpGenerator struct {

}

func (g *OpGenerator) Run (opChan chan<- *Op)  {
	s2 := rand.NewSource(time.Now().Unix())
	r = rand.New(s2)

	actives := []*generator{}
	if updateQps > 0 {
		g := newGenerator(updateQps, updateOp)
		actives = append(actives, g)
	}

	if createQps > 0 {
		g := newGenerator(createQps, createOp)
		actives = append(actives, g)
	}

	if listQps > 0 {
		g := newGenerator(listQps, listOp)
		actives = append(actives, g)
	}

	if getQps > 0 {
		g := newGenerator(getQps, listOp)
		actives = append(actives, g)
	}

	// initial data
	for i := 0; i < int(preCreateNum); i++ {
		op := createOp()
		opChan <- op
	}

	for _, g := range actives {
		g.start(benchMode)
	}

	for {
		now := time.Now()
		if g, nextTime := getActiveOp(actives, now); g!= nil {
			//fmt.Printf("%+v,%+v,%+v,\n",g.exceptTime, g.op,nextTime)
			opChan <- g.op()
			lastest := time.Now()
			duration := lastest.Sub(now)
			if duration < nextTime {
				time.Sleep(nextTime - duration)
			}
		} else {
			time.Sleep(nextTime)
		}
	}
}


func getActiveOp(actives glist, now time.Time) (*generator, time.Duration) {
	if len(actives) == 0 {
		return nil, time.Second
	}
	sort.Sort(actives)
	op := actives[0]
	if op.checkAndUpdate(now) {
		sort.Sort(actives)
		if actives[0].exceptTime.After(now) {
			return op, actives[0].exceptTime.Sub(now)
		} else {
			return op, time.Nanosecond
		}
	} else {
		return nil, op.exceptTime.Sub(now)
	}
}

type glist []*generator

func (g glist) Len() int {
	return len(g)
}

func (g glist) Swap(i,j int) {
	g[i], g[j] = g[j], g[i]
}

func (g glist) Less(i,j int) bool {
	return g[i].exceptTime.Before(g[j].exceptTime)
}
