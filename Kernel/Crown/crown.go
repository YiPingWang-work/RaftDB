package Crown

import (
	"RaftDB/Custom/DB/KVDB"
	"RaftDB/Kernel/Pipe/Something"
	"log"
)

/*
上层模块，上层模块会接受来自logic层的消息，并对此作出处理，Crown和Logic之间使用ToCrownChan和FromCrownChan通讯。
只有客户端的请求会被传送到这里，所以需要对Logic.me中的processClient作出拦截或修正此代码
（当前实现的是只有leader可以响应客户端请求，同时对客户端的所有请求进行follower同步处理）
*/

type Crown struct {
	app           App
	toLogicChan   chan<- Something.Something // 上层接口
	fromLogicChan <-chan Something.Something // 上层接口
}

type App interface {
	Process(in string) (out string, agreeNext bool, err error)
	UndoProcess(in string) (out string, agreeNext bool, err error) // 处理逆信息
	Init()
	ToString() string
}

func (c *Crown) Init(fromLogicChan <-chan Something.Something, toLogicChan chan<- Something.Something) {
	c.toLogicChan, c.fromLogicChan = toLogicChan, fromLogicChan
	c.app = &KVDB.KVDB{}
	c.app.Init()
}

func (c *Crown) Run() {
	for {
		select {
		case sth, opened := <-c.fromLogicChan:
			if !opened {
				log.Println("error: logic chan closed")
			}
			if len(sth.Content) == 0 {
				log.Println("Crown: empty Something.V error")
			} else if sth.Content[0] == '!' {
				if out, agree, err := c.app.UndoProcess(sth.Content); err != nil {
					log.Println(err)
				} else if sth.NeedReply {
					sth.Content, sth.Agree = out, agree
					c.toLogicChan <- sth
				}
			} else {
				if out, agree, err := c.app.Process(sth.Content); err != nil {
					log.Println(err)
				} else if sth.NeedReply {
					sth.Content, sth.Agree = out, agree
					c.toLogicChan <- sth
				}
			}
		}
	}
}

func (c *Crown) ToString() string {
	return c.app.ToString()
}
