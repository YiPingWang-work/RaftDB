package Bottom

import (
	"RaftDB/Kernel/Pipe/Order"
)

type Communicate struct {
	cable Cable
	dns   []string
	addr  string
}

/*
信道接口，实现的通信信道需要实现初始化、回应客户端信息，回复服务节点信息，模拟网络延迟的功能。
*/

type Cable interface {
	Init(cableParam interface{}) error
	ReplyNode(addr string, msg interface{}) error
	Listen(addr string) error
	ReplyClient(msg interface{}) error
	ChangeNetworkDelay(delay int, random bool)
}

/*
通讯系统初始化，实例化自己的信道类型，保存本机的addr和所有通讯节点的映射信息；将上层传过来的信道实例初始化。
*/

func (c *Communicate) init(cable Cable, addr string, dns []string, cableParam interface{}) error {
	c.cable, c.addr, c.dns = cable, addr, dns
	return c.cable.Init(cableParam)
}

func (c *Communicate) replyNode(msg Order.Message) (err error) {
	for _, v := range msg.To {
		if addr := c.dns[v]; addr != c.addr {
			go func() {
				err = c.cable.ReplyNode(addr, msg)
			}()
		}
	}
	return err
}

func (c *Communicate) listen() error {
	return c.cable.Listen(c.addr)
}

func (c *Communicate) ReplyClient(msg Order.Message) error {
	return c.cable.ReplyClient(msg)
}

func (c *Communicate) ChangeNetworkDelay(delay int, random bool) {
	c.cable.ChangeNetworkDelay(delay, random)
}
