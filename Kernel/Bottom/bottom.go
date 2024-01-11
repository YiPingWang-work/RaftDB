package Bottom

import (
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Meta"
	"RaftDB/Kernel/Pipe/Order"
	"log"
)

type Bottom struct {
	communicate   Communicate
	store         Store
	logs          *Log.LogSet
	fromLogicChan <-chan Order.Order // 接收me消息的管道
	toLogicChan   chan<- Order.Order // 发送消息给me的管道
}

/*
bottom初始化，它需要完成：
	1.根据指定的配置文件位置和日志持计划位置将配置和日志读出放在内存，同时将二者作为传出参数返回上层
	2.使用给定的存储介质实体和网线实体初始化自己的存储系统和通信系统
	3.保存自己和Logic层的通讯管道
	4.保存日志系统，注意它和Logic层都有对日志系统的读写权限
*/

func (b *Bottom) Init(confPath string, filePath string, meta *Meta.Meta, logs *Log.LogSet,
	medium Medium, cable Cable,
	fromLogicChan <-chan Order.Order, toLogicChan chan<- Order.Order,
	mediumParam interface{}, cableParam interface{}) {

	b.store, b.logs = Store{}, logs
	b.fromLogicChan, b.toLogicChan = fromLogicChan, toLogicChan
	if err := b.store.initAndLoad(confPath, filePath, meta, logs, medium, mediumParam); err != nil {
		panic(err)
	}
	if err := b.communicate.init(cable, meta.Dns[meta.Id], meta.Dns[0:meta.Num], cableParam); err != nil {
		panic(err)
	}
	logs.Init(meta.CommittedKeyTerm, meta.CommittedKeyIndex)
}

/*
运行期间不断收取Logic层传过来的信息，进行处理。
*/

func (b *Bottom) Run() {
	go func() {
		err := b.communicate.listen()
		if err != nil {
			panic(err)
		}
	}()
	for {
		select {
		case order, opened := <-b.fromLogicChan:
			if !opened {
				panic("panic: logic chan is closed")
				return
			}
			if order.Type == Order.Store {
				if order.Msg.Agree {
					log.Println("store: update meta")
					if err := b.store.updateMeta(order.Msg.Log); err != nil {
						log.Println(err)
					}
				} else {
					log.Printf("store: write log from %d to %d\n", order.Msg.SecondLastLogKey, order.Msg.LastLogKey)
					contents := b.logs.GetLogsByRange(order.Msg.SecondLastLogKey, order.Msg.LastLogKey)
					if err := b.store.appendLogs(&contents); err != nil {
						log.Println(err)
					}
				}
			}
			if order.Type == Order.NodeReply {
				if err := b.communicate.replyNode(order.Msg); err != nil {
					log.Println(err)
				}
			}
			if order.Type == Order.ClientReply {
				if err := b.communicate.ReplyClient(order.Msg); err != nil {
					log.Println(err)
				}
			}
		}
	}
}

func (b *Bottom) ChangeNetworkDelay(delay int, random bool) {
	b.communicate.ChangeNetworkDelay(delay, random)
}
