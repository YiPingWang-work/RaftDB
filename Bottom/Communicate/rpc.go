package Communicate

import (
	"RaftDB/Order"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type RPC struct {
	clientChans        sync.Map
	replyChan          chan<- Order.Order
	networkDelay       time.Duration
	networkDelayRandom int
	num                atomic.Int32
}

func (r *RPC) send(myAddr string, yourAddr string, msg Order.Message) {
	if myAddr == yourAddr {
		return
	}
	client, err := rpc.Dial("tcp", yourAddr)
	if err != nil {
		return
	}
	defer client.Close()
	time.Sleep(r.networkDelay)
	if err = client.Call("RPC.Push", msg, nil); err != nil {
		log.Println(err)
	}
}

func (r *RPC) listenAndServe(myAddr string, replyChan chan<- Order.Order) error {
	r.clientChans, r.replyChan = sync.Map{}, replyChan
	r.changeNetworkDelay(0, 0)
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

func (r *RPC) changeNetworkDelay(delay int, random int) {
	if random == 0 {
		r.networkDelay = time.Duration(delay) * time.Millisecond
	} else {
		r.networkDelay = time.Duration(rand.Intn(random)*delay) * time.Millisecond
	}
}

func (r *RPC) replyClient(msg Order.Message) {
	ch, ok := r.clientChans.Load(msg.From)
	if ok {
		select {
		case ch.(chan Order.Message) <- msg:
		default:
		}
	}
}

func (r *RPC) Push(rec Order.Message, rep *string) error {
	r.replyChan <- Order.Order{Type: Order.FromNode, Msg: rec}
	return nil
}

func (r *RPC) Write(rec Order.Message, rep *string) error {

	rec.From = int(r.num.Add(1))
	ch := make(chan Order.Message, 0)
	r.clientChans.Store(rec.From, ch)
	r.replyChan <- Order.Order{Type: Order.FromClient, Msg: rec}
	timer := time.After(time.Duration(rec.Term) * time.Millisecond)
	select {
	case msg := <-ch:
		*rep = msg.Log
	case <-timer:
		*rep = "timeout"
	}
	close(ch)
	r.clientChans.Delete(rec.From)
	return nil
}
