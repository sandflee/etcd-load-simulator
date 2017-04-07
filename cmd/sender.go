package cmd

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"os"
)

type sender struct {
	sendChan chan *Op
	requests chan *Op
	clients []*client
}

type client struct {
	c *clientv3.Client
}

func NewSender() *sender {
	s := &sender{}
	//s.requests = newQueue()
	s.requests = make(chan *Op, 1024)
	conns := make([]*clientv3.Client, totalConns)
	for i, _ := range conns {
		conns[i] = MustCreateConn(endpoints)
	}
	s.clients = make([]*client, totalClients)
	for i, _ := range s.clients {
		c := &client{}
		if len(conns) == 1 {
			c.c = conns[0]
		} else {
			c.c = conns[i%(len(conns) - 1)]
		}
		s.clients[i] = c
	}
	return s
}

func MustCreateConn(endpoint []string) *clientv3.Client {
	cfg := clientv3.Config{Endpoints: endpoint}
	client, err := clientv3.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}
	return client
}

func (s *sender) Run() {
	for _, client := range s.clients {
		go client.Run(s.requests)
	}

	//s.monitorRequests(requestChan)
}

/*
func (s *sender) monitorRequests(requestsCh chan<-*Op) {
	for {
		op := s.requests.pop()
		requestsCh <- op.(*Op)
		if s.requests.len() > 10 {
			fmt.Printf("too many pending requests,%d\n",s.requests.len())
		}
	}
}
*/


func (c *client) Run (requests <-chan *Op) {
	//fmt.Printf("running client\n")
	context := context.Background()
	for op := range requests {
		if op.del != nil {
			if _, err := c.c.KV.Delete(context, op.del.key); err != nil {
				fmt.Errorf("delete failed",err)
			}
		}

		if op.get != nil {
			opts := []clientv3.OpOption{}
			if !quorumRead {
				opts = append(opts, clientv3.WithSerializable())
			}
			if _, err := c.c.KV.Get(context, op.get.key, opts...); err != nil {
				fmt.Errorf("get failed", err)
			}
		}

		if op.list != nil {
			opts := []clientv3.OpOption{}
			if !quorumRead {
				opts = append(opts, clientv3.WithSerializable())
			}
			opts = append(opts, clientv3.WithPrefix())
			if _, err := c.c.Get(context, op.list.prefix, opts...); err != nil {
				etcd_counter.list(false)
				fmt.Errorf("list failed", err)
			} else {
				etcd_counter.list(true)
			}
		}

		if op.update != nil {
			resp, err := c.c.KV.Get(context, op.update.key)
			if err != nil {
				etcd_counter.update(false)
				fmt.Errorf("update failed while do get,\n",err)
				continue
			}
			if len(resp.Kvs) == 0 {
				etcd_counter.update(false)
				fmt.Errorf("update failed while do get empty\n",err)
				continue
			}
			modIndex := resp.Kvs[0].ModRevision
			txnResp, err := c.c.KV.Txn(context).If(
				clientv3.Compare(clientv3.ModRevision(op.update.key), "=", modIndex),
			).Then(
				clientv3.OpPut(op.update.key, string(op.update.value)),
			).Else(
				clientv3.OpGet(op.update.key),
			).Commit()

			if err != nil {
				etcd_counter.update(false)
				fmt.Errorf("update failed ,",err)
				continue
			}

			if !txnResp.Succeeded {
				etcd_counter.update(false)
				fmt.Errorf("update not succ,", op.update.key)
				continue
			}
			etcd_counter.update(true)
		}

		if op.put != nil {
			key := op.put.key
			value := op.put.value
			_, err := c.c.KV.Txn(context).If(
				notFound(key),
			).Then(
				clientv3.OpPut(key, value),
			).Commit()
			if err != nil {
				etcd_counter.create(false)
				fmt.Errorf("put error,",err)
			} else {
				etcd_counter.create(true)
			}
		}
	}
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func (s *sender) addOp(op *Op) {
	//s.requests.push(op)
	s.requests <-op
}

func (s *sender) pendingOp() int {
	return len(s.requests)
}
