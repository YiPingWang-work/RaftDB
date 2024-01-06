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
	replyClient(msg Order.Msg)
	changeNetworkDelay(delay int, random int)
}

func (c *Communicate) Init(cable Cable, addr string, dns []string) {
	c.cable, c.addr, c.dns = cable, addr, dns
}

func (c *Communicate) Send(msg Order.Msg) error {
	for _, v := range msg.To {
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

func (c *Communicate) ReplyClient(msg Order.Msg) {
	c.cable.replyClient(msg)
}

func (c *Communicate) ChangeNetworkDelay(delay int, random int) {
	c.cable.changeNetworkDelay(delay, random)
}
