package Logic

import (
	"RaftDB/Log"
	"RaftDB/Order"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

var leader Leader

type Leader struct {
	agree map[Log.LogKeyType]map[int]bool // 对于哪条记录，同意的follower集合
	index int                             // 当前日志的index
}

func (l *Leader) init(me *Me) error {
	l.agree, l.index = map[Log.LogKeyType]map[int]bool{}, 0
	return l.processTimeout(me)
}

func (l *Leader) processHeartbeat(msg Order.Msg, me *Me) error {
	return errors.New("error: maybe two leaders")
}

func (l *Leader) processRequest(msg Order.Msg, me *Me) error {
	return errors.New("error: maybe two leaders")
}

func (l *Leader) processRequestReply(msg Order.Msg, me *Me) error { // 处理回复
	reply := Order.Msg{
		Type:       -1,
		From:       me.meta.Id,
		To:         []int{},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if me.logs.GetLast().Less(msg.LastLogKey) { // 如果follower存在比我大的日志，报错
		return errors.New("error: a follower has greater key")
	}
	if msg.Agree == true { // 如果follower同意我的请求
		if !me.logs.GetCommitted().Less(msg.LastLogKey) { // 如果是一个leader已经提交的，leader对其返回一个提交
			reply.Type, reply.To = Order.Commit, []int{msg.From}
			if l.agree[msg.LastLogKey] != nil { // 如果自己的同意列表中还有这条数据记录，释放空间
				delete(l.agree, msg.LastLogKey)
			}
			log.Printf("Leader: %d should commit my committed log %v\n", msg.From, msg.LastLogKey)
		} else { // 如果是一个leader没有提交的，增加票数
			l.agree[msg.LastLogKey][msg.From] = true
			if len(l.agree[msg.LastLogKey]) >= me.quorum && me.meta.Term == msg.LastLogKey.Term { // 如果票数超过quorum同时是自己任期发出的，则leader提交，通知所有的follower提交
				previousCommitted := me.logs.Commit(msg.LastLogKey)
				me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = me.logs.GetCommitted().Term, me.logs.GetCommitted().Index
				me.replyChan <- Order.Order{
					Type: Order.Store,
					Msg: Order.Msg{
						Agree:            false,
						LastLogKey:       me.logs.GetCommitted(),
						SecondLastLogKey: me.logs.GetNext(previousCommitted),
					}}
				me.replyChan <- Order.Order{Type: Order.ClientReply, Msg: Order.Msg{Agree: true}} // 向客户端返回正确
				if metaTmp, err := json.Marshal(*me.meta); err != nil {
					return err
				} else {
					me.replyChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(metaTmp)}}
				}
				reply.Type = Order.Commit
				reply.To = me.members
				me.timer = time.After(me.leaderHeartbeat)
				log.Printf("Leader: quorum have agree key %v, I will commit and boardcast commit\n", msg.LastLogKey)
			}
		}
		nextKey := me.logs.GetNext(msg.LastLogKey) // 如果leader还有数据需要同步，leader尝试发送下一条
		if nextKey.Term != -1 {
			req, err := me.logs.GetContentByKey(nextKey)
			if err != nil {
				return err
			}
			me.replyChan <- Order.Order{Type: Order.Send, Msg: Order.Msg{
				Type:             Order.Request,
				From:             reply.From,
				To:               []int{msg.From},
				Term:             reply.Term,
				LastLogKey:       nextKey,
				SecondLastLogKey: msg.LastLogKey,
				Log:              req,
			}}
			log.Printf("Leader: %d accept my request %v, but %d's logs is not complete, send request %v\n",
				msg.From, msg.LastLogKey, msg.From, nextKey)
			if nextKey.Term == 1 {
				log.Println(msg)
			}
		}
	} else { // 如果follower不同意我的请求，说明数据未对齐
		newLastLogKey := me.logs.GetLast()
		if msg.LastLogKey.Term != -1 {
			newLastLogKey = me.logs.GetPrevious(msg.LastLogKey) // 尝试发送上一条
			log.Printf("Leader: %d refuse my request %v, his logs is not complete, send request %v\n",
				msg.From, msg.LastLogKey, newLastLogKey)
		} else {
			log.Printf("Leader: %d's log is not complete, but I don't know his step, send request %v\n",
				msg.From, newLastLogKey)
		}
		if newLastLogKey.Term == -1 { // 没有上一条，不应该不同意，返回错误
			return errors.New("error: follower disagree first log")
		}
		reply.Type, reply.To = Order.Request, []int{msg.From} // 继续发送数据
		reply.LastLogKey = newLastLogKey
		reply.SecondLastLogKey = me.logs.GetPrevious(newLastLogKey)
		if req, err := me.logs.GetContentByKey(newLastLogKey); err != nil {
			return err
		} else {
			reply.Log = req
		}
	}
	if len(reply.To) != 0 { // 如果有需要发送，发送
		me.replyChan <- Order.Order{Type: Order.Send, Msg: reply}
	}
	return nil
}

func (l *Leader) processCommit(msg Order.Msg, me *Me) error {
	return errors.New("error: maybe two leaders")
}

func (l *Leader) processVote(msg Order.Msg, me *Me) error { // 如果收到一个同级选举，立即发送一次心跳
	return l.processTimeout(me)
}

func (l *Leader) processVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

func (l *Leader) processPreVote(msg Order.Msg, me *Me) error { // 如果收到一个同级预选举，立即发送一次心跳
	return l.processTimeout(me)
}

func (l *Leader) processPreVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

func (l *Leader) processClient(req Log.LogType, me *Me) error { // 处理一个客户端请求
	secondLastKey := me.logs.GetLast()                               // 获取我的最后一个日志
	lastLogKey := Log.LogKeyType{Term: me.meta.Term, Index: l.index} // 创建一个新的日志
	me.logs.Append(Log.Content{LogKey: lastLogKey, Log: req})        // 将日志添加到本地
	l.agree[lastLogKey] = map[int]bool{}                             // 为agree列表添加一笔请求
	me.replyChan <- Order.Order{Type: Order.Send, Msg: Order.Msg{
		Type:             Order.Request,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		LastLogKey:       lastLogKey,
		SecondLastLogKey: secondLastKey,
		Log:              req,
	}}
	me.timer = time.After(me.leaderHeartbeat)
	l.index++ // 预index++
	log.Printf("Leader: reveive a client's request, key: %v, log: %v, now I will broadcast this request\n", lastLogKey, req)
	return nil
}

func (l *Leader) processTimeout(me *Me) error {
	me.replyChan <- Order.Order{Type: Order.Send, Msg: Order.Msg{
		Type:             Order.Heartbeat,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		LastLogKey:       me.logs.GetLast(),
		SecondLastLogKey: me.logs.GetSecondLast(),
	}}
	me.timer = time.After(me.leaderHeartbeat)
	log.Println("Leader: timeout")
	return nil
}

func (l *Leader) ToString() string {
	res := fmt.Sprintf("==== LEADER ====\nindex: %d\nagreedReply:\n", l.index)
	for k, v := range l.agree {
		s := fmt.Sprintf("	key: {%d %d} -> ", k.Term, k.Index)
		for k2, _ := range v {
			s += fmt.Sprintf("%d ", k2)
		}
		res += s + "\n"
	}
	return res + "==LEADER=="
}
