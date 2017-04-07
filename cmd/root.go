package cmd

import (
	"github.com/spf13/cobra"
	"time"
	"fmt"
	"github.com/golang/glog"
	"runtime"
	"golang.org/x/net/context"
	"github.com/coreos/etcd/clientv3"
)

// This represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "etcd-sim",
	Short: "A low-level simulater tool for etcd3",
	Long: `benchmark is a low-level benchmark tool for etcd3.
It uses gRPC client directly and does not depend on
etcd client library.
	`,
	Run: doRun,
}

var (
	endpoints    []string
	totalConns   uint
	totalClients uint
	updateQps	 uint
	createQps	 uint
	listQps	 uint
	delQps	 uint
	getQps	 uint
	quorumRead bool
	preCreateNum uint
)

func init() {
	RootCmd.PersistentFlags().StringSliceVar(&endpoints, "endpoints", []string{"127.0.0.1:2379"}, "gRPC endpoints")
	RootCmd.PersistentFlags().UintVar(&totalConns, "conns", 1, "Total number of gRPC connections")
	RootCmd.PersistentFlags().UintVar(&totalClients, "clients", 1, "Total number of gRPC clients")
	RootCmd.PersistentFlags().UintVar(&updateQps, "updateQps", 0, "")
	RootCmd.PersistentFlags().UintVar(&createQps, "createQps", 0, "")
	RootCmd.PersistentFlags().UintVar(&listQps, "listQps", 0, "")
	RootCmd.PersistentFlags().UintVar(&delQps, "delQps", 0, "")
	RootCmd.PersistentFlags().UintVar(&getQps, "getQps", 0, "")
	RootCmd.PersistentFlags().BoolVar(&quorumRead, "quorumRead", true, "")
	RootCmd.PersistentFlags().UintVar(&preCreateNum, "preCreate", 1000, "")
}

func initEtcdContent()  {
	client := MustCreateConn(endpoints)

	fmt.Printf("del all etcd keys with prefix:" + prefix)
	context := context.Background()
	rsp, err := client.KV.Delete(context, prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Errorf("del etcd keys failed,", err)
	}
	if (rsp != nil) {
		fmt.Printf("delete %d keys\n", rsp.Deleted)
	}
}

func doRun(cmd *cobra.Command, args []string) {
	defer glog.Flush()

	opChan := make(chan *Op)

	g := OpGenerator{}
	sender := NewSender()

	go g.Run(opChan)
	go sender.Run()

	initEtcdContent()

	glog.V(1).Info("do main loop\n")
	fmt.Printf("do main loop, endpoints:%s\n", endpoints)
	fmt.Printf("createQps:%d,updateQps:%d,listQps:%d,delQps:%d,getQps:%d\n", createQps, updateQps, listQps, delQps, getQps)

	ticker := time.NewTicker(2000*time.Millisecond)
	buf := make([]byte, 102400)
	for {
		select {
		case op := <-opChan:
			sender.addOp(op)
		case <-ticker.C:
			fmt.Printf("pending:%d,time:%s,%+v\n", sender.pendingOp(),time.Now().Local().String(), etcd_counter)
			runtime.Stack(buf, true)
			//fmt.Printf(string(buf))
		}
	}
}