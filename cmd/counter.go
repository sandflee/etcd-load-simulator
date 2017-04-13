package cmd

import (
	"sync/atomic"
	"fmt"
)

var etcd_counter = counter{}

type counter struct {
	create_num uint64
	list_num uint64
	update_num uint64
	get_num uint64
	fail_create_num uint64
	fail_list_num uint64
	fail_update_num uint64
	fail_get_num uint64
}

func (c *counter) create(succ bool) {
	if !succ {
		atomic.AddUint64(&c.fail_create_num, 1)
	}
	atomic.AddUint64(&c.create_num, 1)
}

func (c *counter) update(succ bool) {
	if !succ {
		atomic.AddUint64(&c.fail_update_num, 1)
	}
	atomic.AddUint64(&c.update_num, 1)
}

func (c *counter) list(succ bool) {
	if !succ {
		atomic.AddUint64(&c.fail_list_num, 1)
	}
	atomic.AddUint64(&c.list_num, 1)
}

func (c *counter) get(succ bool) {
	if !succ {
		atomic.AddUint64(&c.fail_get_num, 1)
	}
	atomic.AddUint64(&c.get_num, 1)
}

func (c counter) String() string {
	return fmt.Sprintf("total:%d, create: %d,create_failed: %d,update: %d,update_failed: %d, list: %d,list_failed:%d",(c.create_num + c.update_num + c.list_num), c.create_num,c.fail_create_num,c.update_num,c.fail_update_num,c.list_num,c.fail_list_num)
}




