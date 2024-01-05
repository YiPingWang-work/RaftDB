package Communicate

import (
	"RaftDB/Order"
	"fmt"
)

type Communicate struct {
	cable Cable
	dns   []string
	addr  string
}

type Cable interface {
	send(myAddr string, yourAddr string, msg Order.Msg)
	listenAndServe(myAddr string, replyChan chan<- Order.Order) error
	changeClientLicence(st bool)
	replyClient(st bool)
}

func (c *Communicate) Init(cable Cable, addr string, dns []string) {
	c.cable = cable
	c.addr = addr
	c.dns = dns
}

func (c *Communicate) Send(members []int, msg Order.Msg) error {
	for _, v := range members {
		go c.cable.send(c.addr, c.dns[v], msg)
	}
	return nil
}

func (c *Communicate) ListenAndServe(replyChan chan<- Order.Order) {
	if err := c.cable.listenAndServe(c.addr, replyChan); err != nil {
		fmt.Println(err)
	}
}

func (c *Communicate) ChangeClientLicence(st bool) {
	c.cable.changeClientLicence(st)
}

func (c *Communicate) ReplyClient(st bool) {
	c.cable.replyClient(st)
}
