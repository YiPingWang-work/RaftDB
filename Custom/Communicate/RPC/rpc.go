package RPC

import (
	"RaftDB/Kernel/Pipe/Order"
	"errors"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

/*
	type Cable interface {
		Init(cableParam interface{}) error
		ReplyNode(addr string, msg interface{}) error
		Listen(addr string) error
		ReplyClient(msg interface{}) error
		ChangeNetworkDelay(delay int, random bool)
	}
*/
/*
要求rpc将信息推送到Logic层的时候，必须为消息打上全局唯一的标识。
*/

type RPC struct {
	clientChans sync.Map
	replyChan   chan<- Order.Order
	delay       time.Duration
	num         atomic.Int32
}

func (r *RPC) Init(replyChan interface{}) error {
	if x, ok := replyChan.(chan Order.Order); !ok {
		return errors.New("RPC: Init need a reply chan")
	} else {
		r.replyChan = x
	}
	r.clientChans = sync.Map{}
	r.ChangeNetworkDelay(0, false)
	if err := rpc.RegisterName("RPC", r); err != nil {
		return err
	}
	return nil
}

func (r *RPC) ReplyNode(addr string, msg interface{}) error {
	if x, ok := msg.(Order.Message); !ok {
		return errors.New("RPC: ReplyNode need a Order.Message")
	} else {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return err
		}
		defer client.Close()
		time.Sleep(r.delay)
		if err = client.Call("RPC.Push", x, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *RPC) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
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

func (r *RPC) ChangeNetworkDelay(delay int, random bool) {
	if !random {
		r.delay = time.Duration(delay) * time.Millisecond
	} else {
		r.delay = time.Duration(rand.Intn(delay)) * time.Millisecond
	}
}

func (r *RPC) ReplyClient(msg interface{}) error {
	if x, ok := msg.(Order.Message); !ok {
		return errors.New("RPC: ReplyClient need a Order.Message")
	} else {
		ch, ok := r.clientChans.Load(x.From)
		if ok {
			select {
			case ch.(chan Order.Message) <- x:
			default:
			}
		}
	}
	return nil
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
