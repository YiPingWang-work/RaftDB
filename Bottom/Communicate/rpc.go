package Communicate

import (
	"RaftDB/Order"
	"log"
	"net"
	"net/rpc"
	"time"
)

type RPC struct {
	clientLicence bool
	clientChan    chan bool
	replyChan     chan<- Order.Order
}

func (r *RPC) send(myAddr string, yourAddr string, msg Order.Msg) {
	if myAddr == yourAddr {
		return
	}
	client, err := rpc.Dial("tcp", yourAddr)
	if err != nil {
		return
	}
	_ = client.Call("RPC.Push", msg, nil)
}

func (r *RPC) listenAndServe(myAddr string, replyChan chan<- Order.Order) error {
	r.clientChan = make(chan bool, 1000)
	r.replyChan = replyChan
	if err := rpc.RegisterName("RPC", r); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		if conn, err := listener.Accept(); err != nil {
			log.Println(err)
		} else {
			go rpc.ServeConn(conn)
		}
	}
}

func (r *RPC) changeClientLicence(st bool) {
	r.clientLicence = st
}

func (r *RPC) replyClient(st bool) {
	r.clientChan <- st
}

func (r *RPC) Push(rec Order.Msg, rep *string) error {
	r.replyChan <- Order.Order{Type: Order.FromNode, Msg: rec}
	return nil
}

func (r *RPC) Write(rec Order.Msg, rep *string) error {
	r.replyChan <- Order.Order{Type: Order.FromClient, Msg: rec}
	timer := time.After(time.Duration(rec.Term) * time.Millisecond)
	if r.clientLicence {
		select {
		case ok := <-r.clientChan:
			if ok {
				*rep = "ok"
			} else {
				*rep = "refuse"
			}
		case <-timer:
			*rep = "timeout"
		}
	} else {
		*rep = "client --x-> follower/candidate"
	}
	return nil
}
