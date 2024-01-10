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

func (b *Bottom) Init(confPath string, filePath string, meta *Meta.Meta, logs *Log.LogSet,
	medium Medium, cable Cable,
	fromLogicChan <-chan Order.Order, toLogicChan chan<- Order.Order,
	mediumParam interface{}, cableParam interface{}) {

	b.store, b.logs = Store{}, logs
	b.fromLogicChan, b.toLogicChan = fromLogicChan, toLogicChan
	if err := b.store.initAndLoad(confPath, filePath, meta, logs, medium, mediumParam); err != nil {
		log.Println(err)
	}
	if err := b.communicate.init(cable, meta.Dns[meta.Id], meta.Dns[0:meta.Num], cableParam); err != nil {
		log.Println(err)
	}
	logs.Init(meta.CommittedKeyTerm, meta.CommittedKeyIndex)
}

func (b *Bottom) Run() {
	go b.communicate.listen()
	for {
		select {
		case order, opened := <-b.fromLogicChan:
			if !opened {
				log.Println("error: logic chan is closed")
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
					contents := b.logs.GetVsByRange(order.Msg.SecondLastLogKey, order.Msg.LastLogKey)
					if err := b.store.appendLogs(&contents); err != nil {
						log.Println(err)
					}
				}
			}
			if order.Type == Order.NodeReply {
				if err := b.communicate.send(order.Msg); err != nil {
					log.Println(err)
				}
			}
			if order.Type == Order.ClientReply {
				b.communicate.ReplyClient(order.Msg)
			}
		}
	}
}

func (b *Bottom) ChangeNetworkDelay(delay int, random bool) {
	b.communicate.ChangeNetworkDelay(delay, random)
}
