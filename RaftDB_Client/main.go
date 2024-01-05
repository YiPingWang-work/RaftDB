package main

import (
	"RaftDB_Client/MsgLog"
	"log"
	"net/rpc"
	"os"
)

func main() {
	addr := os.Args[1]
	content := os.Args[2]
	if len(content) == 0 {
		return
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
	}
	req := MsgLog.Msg{Log: MsgLog.LogType(content), Term: 50000}
	rep := ""
	if err := client.Call("RPC.Write", req, &rep); err != nil {
		log.Println(err)
	}
	log.Println(rep)
}
