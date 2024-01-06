package main

import (
	"RaftDB_Client/MsgLog"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

func main() {
	addr := os.Args[1]
	content := os.Args[2]
	timeout, err := strconv.Atoi(os.Args[3])
	if len(content) == 0 || err != nil {
		return
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
	}
	req := MsgLog.Msg{Log: MsgLog.LogType(content), Term: timeout}
	rep := ""
	if err := client.Call("RPC.Write", req, &rep); err != nil {
		log.Println(err)
	}
	log.Println(rep)
}
