package Crown

import (
	"RaftDB/Kernel/Log"
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

/*
App接口需要实现初始化、操作与逆操作的功能。
*/

type App interface {
	Process(in string) (out string, agreeNext bool, err error)
	UndoProcess(in string) (out string, agreeNext bool, err error) // 处理逆信息
	ChangeProcessDelay(delay int, random bool)
	Init()
	ToString() string
}

/*
crown初始化，保存获取Logic层和crown层的通讯管道，初始化APP，应用Logic层的日志。
*/

func (c *Crown) Init(logSet *Log.LogSet, app App, fromLogicChan <-chan Something.Something, toLogicChan chan<- Something.Something) {
	c.toLogicChan, c.fromLogicChan = toLogicChan, fromLogicChan
	c.app = app
	c.app.Init()
	for _, v := range logSet.GetAll() {
		if _, ok, err := c.app.Process(v.V); err != nil || !ok {
			log.Println("error: process history log error")
		}
	}
}

/*
开始监听通讯管道，如果有消息处理，处理，如果该消息需要回复，将结果回复。
*/

func (c *Crown) Run() {
	for {
		select {
		case sth, opened := <-c.fromLogicChan:
			if !opened {
				panic("panic: logic chan closed")
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

func (c *Crown) ChangeProcessDelay(delay int, random bool) {
	c.app.ChangeProcessDelay(delay, random)
}

func (c *Crown) ToString() string {
	return c.app.ToString()
}
