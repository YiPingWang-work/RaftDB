package Bottom

import (
	"RaftDB/Bottom/Communicate"
	"RaftDB/Bottom/Store"
	"RaftDB/Log"
	"RaftDB/Meta"
	"RaftDB/Order"
	"log"
)

type Bottom struct {
	communicate   Communicate.Communicate
	store         Store.Store
	logs          *Log.Logs
	fromLogicChan <-chan Order.Order // 接收me消息的管道
	toLogicChan   chan<- Order.Order // 发送消息给me的管道
}

func (b *Bottom) Init(confPath string, filePath string, meta *Meta.Meta, logs *Log.Logs,
	fromLogicChan <-chan Order.Order, toLogicChan chan<- Order.Order) {
	b.store, b.logs = Store.Store{}, logs
	b.fromLogicChan, b.toLogicChan = fromLogicChan, toLogicChan
	var tmp []string
	b.store.Init(new(Store.CommonFile), confPath, filePath, meta, &tmp)
	for _, v := range tmp {
		if res, err := Log.StringToLog(v); err == nil {
			logs.Append(res)
		}
	}
	logs.Init(meta.CommittedKeyTerm, meta.CommittedKeyIndex)
	b.communicate.Init(new(Communicate.RPC), meta.Dns[meta.Id], meta.Dns[0:meta.Num])
}

func (b *Bottom) Run() {
	go b.communicate.ListenAndServe(b.toLogicChan)
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
					if err := b.store.UpdateMeta(string(order.Msg.Log)); err != nil {
						log.Println(err)
					}
				} else {
					log.Printf("store: write log from %d to %d\n", order.Msg.SecondLastLogKey, order.Msg.LastLogKey)
					for _, v := range b.logs.GetLogsByRange(order.Msg.SecondLastLogKey, order.Msg.LastLogKey) {
						if err := b.store.AppendLog(Log.LogToString(v)); err != nil {
							log.Println(err)
						}
					}
				}
			}
			if order.Type == Order.NodeReply {
				if err := b.communicate.Send(order.Msg); err != nil {
					log.Println(err)
				}
			}
			if order.Type == Order.ClientReply {
				b.communicate.ReplyClient(order.Msg)
			}
		}
	}
}

func (b *Bottom) ChangeNetworkDelay(delay int, random int) {
	b.communicate.ChangeNetworkDelay(delay, random)
}
