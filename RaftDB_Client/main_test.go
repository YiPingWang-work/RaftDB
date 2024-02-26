package main

import (
	"RaftDB_Client/DB/KVDB"
	"RaftDB_Client/Msg"
	"fmt"
	"net/rpc"
	"sync"
	"testing"
)

func Test(t *testing.T) {
	db := KVDB.KVDBClient{}
	addr := "localhost:18001"
	pool := sync.Pool{New: func() interface{} {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return nil
		} else {
			return client
		}
	}}
	for i := 500000; i < 600000; i++ {
		i := i
		//go func() {
		content, _ := db.Parser(fmt.Sprintf("write %d %d", i, i+1))
		client, ok := pool.Get().(*rpc.Client)
		if !ok {
			fmt.Println("fuck")
			break
		}
		req := Msg.Msg{Log: Msg.LogType(content), Term: 50000000, Agree: content[1] == 'r'}
		rep := ""
		if err := client.Call("RPC.Write", req, &rep); err != nil {
			fmt.Println(err)
			return
		}
		pool.Put(client)
		fmt.Println(rep)

		//}()
	}
	//time.Sleep(time.Second * 1000)
}
