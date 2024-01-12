package main

import (
	"RaftDB_Client/DB/KVDB"
	"RaftDB_Client/Msg"
	"bufio"
	"fmt"
	"net/rpc"
	"os"
)

func main() {
	db := KVDB.KVDBClient{}
	if len(os.Args) != 2 {
		fmt.Println("input server's addr like [ip]:[host]")
		return
	}
	addr := os.Args[1]
	for {
		order, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		content, ok := db.Parser(order)
		if !ok {
			fmt.Println("illegal operation")
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println(err)
		}
		req := Msg.Msg{Log: Msg.LogType(content), Term: 50000000, Agree: content[1] == 'r'}
		rep := ""
		if err := client.Call("RPC.Write", req, &rep); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(rep)
	}
}
