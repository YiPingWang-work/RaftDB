package Bottom

import (
	"RaftDB/Kernel/Pipe/Order"
	"fmt"
)

type Communicate struct {
	cable Cable
	dns   []string
	addr  string
}

type Cable interface {
	Init(cableParam interface{}) error
	Send(addr string, msg interface{})
	Listen(addr string) error
	ReplyClient(msg interface{})
	ChangeNetworkDelay(delay int, random bool)
}

func (c *Communicate) init(cable Cable, addr string, dns []string, cableParam interface{}) error {
	c.cable, c.addr, c.dns = cable, addr, dns
	return c.cable.Init(cableParam)
}

func (c *Communicate) send(msg Order.Message) error {
	for _, v := range msg.To {
		if addr := c.dns[v]; addr != c.addr {
			go c.cable.Send(addr, msg)
		}
	}
	return nil
}

func (c *Communicate) listen() {
	if err := c.cable.Listen(c.addr); err != nil {
		fmt.Println(err)
	}
}

func (c *Communicate) ReplyClient(msg Order.Message) {
	c.cable.ReplyClient(msg)
}

func (c *Communicate) ChangeNetworkDelay(delay int, random bool) {
	c.cable.ChangeNetworkDelay(delay, random)
}
