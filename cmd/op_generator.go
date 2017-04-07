package cmd

import (
	"time"
	"math/rand"
	"fmt"
	"os"
	"strconv"
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
	lastTime time.Time
}

func (t *timeChecker) checkAndUpdate(now time.Time) bool {
	if now.Before(t.lastTime.Add(t.interval)) {
		return false
	}
	t.lastTime = now
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

	actives := []generator{}
	if updateQps > 0 {
		a := generator{}
		a.timeChecker = &timeChecker{}
		a.timeChecker.interval = time.Second/time.Duration(updateQps)
		a.op = updateOp
		actives = append(actives, a)
	}

	if createQps > 0 {
		a := generator{}
		a.timeChecker = &timeChecker{}
		a.timeChecker.interval = time.Second/time.Duration(createQps)
		a.op = createOp
		actives = append(actives, a)
	}

	if listQps > 0 {
		a := generator{}
		a.timeChecker = &timeChecker{}
		a.timeChecker.interval = time.Second/time.Duration(listQps)
		a.op = listOp
		actives = append(actives, a)
	}

	if getQps > 0 {
		a := generator{}
		a.timeChecker = &timeChecker{}
		a.timeChecker.interval = time.Second/time.Duration(getQps)
		a.op = getOp
		actives = append(actives, a)
	}

	// initial data
	for i := 0; i < int(preCreateNum); i++ {
		op := createOp()
		opChan <- op
	}

	c := time.Tick(time.Millisecond)
	for {
		select {
		case <-c:
			now := time.Now()
			for _, g := range actives {
				if g.checkAndUpdate(now) {
					op := g.op()
					opChan <- op
				}
			}
		}
	}
}
