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
	clientLicence      bool
	clientChans        sync.Map
	replyChan          chan<- Order.Order
	networkDelay       time.Duration
	networkDelayRandom int
	num                atomic.Int32
}

func (r *RPC) send(myAddr string, yourAddr string, msg Order.Msg) {
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
	ch, ok := r.clientChans.Load(msg.To[0])
	if ok {
		select {
		case ch.(chan Order.Msg) <- msg:
		default:
		}
	}
}

func (r *RPC) Push(rec Order.Msg, rep *string) error {
	r.replyChan <- Order.Order{Type: Order.FromNode, Msg: rec}
	return nil
}

func (r *RPC) Write(rec Order.Msg, rep *string) error {
	if r.clientLicence {
		rec.From = int(r.num.Add(1))
		ch := make(chan Order.Msg, 0)
		r.clientChans.Store(rec.From, ch)
		r.replyChan <- Order.Order{Type: Order.FromClient, Msg: rec}
		timer := time.After(time.Duration(rec.Term) * time.Millisecond)
		select {
		case msg := <-ch:
			if msg.Agree {
				*rep = "ok"
			} else {
				*rep = "refuse"
			}
		case <-timer:
			*rep = "timeout"
		}
		close(ch)
		r.clientChans.Delete(rec.From)
		return nil
	}
	*rep = "client --x-> follower/candidate"
	return nil
}
