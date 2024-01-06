package Communicate

import (
	"RaftDB/Order"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"time"
)

type RPC struct {
	clientLicence      bool
	clientChan         chan Order.Msg
	replyChan          chan<- Order.Order
	networkDelay       time.Duration
	networkDelayRandom int
}

func (r *RPC) send(myAddr string, yourAddr string, msg Order.Msg) {
	if myAddr == yourAddr {
		return
	}
	client, err := rpc.Dial("tcp", yourAddr)
	if err != nil {
		return
	}
	time.Sleep(r.networkDelay)
	_ = client.Call("RPC.Push", msg, nil)
}

func (r *RPC) listenAndServe(myAddr string, replyChan chan<- Order.Order) error {
	r.clientChan, r.replyChan = make(chan Order.Msg, 1000), replyChan
	r.changeNetworkDelay(0, 0)
	r.changeClientLicence(false)
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

func (r *RPC) changeNetworkDelay(delay int, random int) {
	if random == 0 {
		r.networkDelay = time.Duration(delay) * time.Millisecond
	} else {
		r.networkDelay = time.Duration(rand.Intn(random)*delay) * time.Millisecond
	}
}

func (r *RPC) replyClient(msg Order.Msg) {
	r.clientChan <- msg
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
		case msg := <-r.clientChan:
			if msg.Agree {
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
