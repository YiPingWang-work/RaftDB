package Crown

import (
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Pipe/Something"
	"log"
)

/*
上层模块，上层模块会接受来自logic层的消息，并对此作出处理，Crown和Logic之间使用ToCrownChan和FromCrownChan通讯。
只有客户端的请求会被传送到这里。
*/

type Crown struct {
	app           App
	toLogicChan   chan<- Something.Something          // 上层接口
	fromLogicChan <-chan Something.Something          // 上层接口
	watchingMap   map[string][]int                    // 存储监听的事件
	watchTrigger  func(string) (bool, string, string) // 监听触发函数，对于一个命令，他可能触发的key是什么，以及返回什么
}

/*
App接口需要实现初始化、操作与逆操作的功能。
*/

type App interface {
	Process(in string) (out string, agree bool, watching bool, err error)
	UndoProcess(in string) (out string, agree bool, err error) // 处理逆信息
	ChangeProcessDelay(delay int, random bool)
	Init() (watchTrigger func(string) (bool, string, string))
	ToString() string
}

/*
crown初始化，保存获取Logic层和crown层的通讯管道，初始化APP，应用Logic层的日志。
*/

func (c *Crown) Init(logSet *Log.LogSet, app App,
	fromLogicChan <-chan Something.Something, toLogicChan chan<- Something.Something) {

	c.toLogicChan, c.fromLogicChan = toLogicChan, fromLogicChan
	c.app, c.watchingMap = app, map[string][]int{}
	c.watchTrigger = c.app.Init()
	for _, v := range logSet.GetAll() {
		if _, ok, _, err := c.app.Process(v.V); err != nil || !ok {
			log.Println("error: process history log error")
		}
	}
}

/*
开始监听通讯管道，如果有消息处理，处理，如果该消息需要回复，将结果回复。
在执行过程中发现通讯管道关闭，Panic返回。
*/

func (c *Crown) Run() {
	for {
		select {
		case sth, opened := <-c.fromLogicChan:
			if !opened {
				panic("logic chan closed")
			}
			if maybe, key, reply := c.watchTrigger(sth.Content); maybe && c.watchingMap[key] != nil {
				for _, v := range c.watchingMap[key] {
					c.toLogicChan <- Something.Something{Id: v, NeedSync: false, Agree: true, Content: reply}
					log.Printf("Crown: %d's watching event '%s' has been triggered\n", v, key)
				}
				delete(c.watchingMap, key)
			}
			if len(sth.Content) > 0 && sth.Content[0] == '!' {
				if out, agree, err := c.app.UndoProcess(sth.Content); err != nil {
					log.Println(err)
				} else if sth.NeedReply {
					sth.Content, sth.Agree = out, agree
					c.toLogicChan <- sth
				}
			} else {
				if out, agree, watching, err := c.app.Process(sth.Content); err != nil {
					log.Println(err)
				} else {
					if watching {
						c.watchingMap[out] = append(c.watchingMap[out], sth.Id)
						log.Printf("Crown: %d registers a watching event '%s'\n", sth.Id, out)
					} else if sth.NeedReply {
						sth.Content, sth.Agree = out, agree
						c.toLogicChan <- sth
					}
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
