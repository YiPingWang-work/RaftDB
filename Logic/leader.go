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
	agree        map[Log.LogKeyType]map[int]bool // 对于哪条记录，同意的follower集合
	client       map[Log.LogKeyType]int          // 对应agree的是哪个client发出的
	agreeAddNode map[string]map[int]bool         // 对于某次节点变更，同意的follower集合
	index        int                             // 当前日志的index
}

func (l *Leader) init(me *Me) error {
	l.agree, l.index = map[Log.LogKeyType]map[int]bool{}, 0
	l.client, l.agreeAddNode = map[Log.LogKeyType]int{}, map[string]map[int]bool{}
	return l.processTimeout(me)
}

func (l *Leader) processHeartbeat(msg Order.Msg, me *Me) error {
	return errors.New("error: maybe two leaders")
}

func (l *Leader) processAppendLog(msg Order.Msg, me *Me) error {
	return errors.New("error: maybe two leaders")
}

func (l *Leader) processAppendLogReply(msg Order.Msg, me *Me) error { // 处理回复
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
			log.Printf("Leader: %d should commit my committed log %v\n", msg.From, msg.LastLogKey)
		} else { // 如果是一个leader没有提交的，增加票数
			l.agree[msg.LastLogKey][msg.From] = true
			if len(l.agree[msg.LastLogKey]) >= me.quorum && me.meta.Term == msg.LastLogKey.Term { // 如果票数超过quorum同时是自己任期发出的，则leader提交，通知所有的follower提交
				previousCommitted := me.logs.Commit(msg.LastLogKey)
				me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = msg.LastLogKey.Term, msg.LastLogKey.Index
				secondLastKey := me.logs.GetNext(previousCommitted)
				me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{
					Agree:            false,
					LastLogKey:       msg.LastLogKey,
					SecondLastLogKey: secondLastKey,
				}}
				for _, v := range me.logs.GetKeysByRange(secondLastKey, msg.LastLogKey) {
					me.toBottomChan <- Order.Order{Type: Order.ClientReply, Msg: Order.Msg{ // 向客户端返回正确，删除记录
						From:  me.meta.Id,
						To:    []int{l.client[v]},
						Agree: true,
					}}
					delete(l.client, v)
					delete(l.agree, v)
				}
				if metaTmp, err := json.Marshal(*me.meta); err != nil {
					return err
				} else {
					me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(metaTmp)}}
				}
				reply.Type, reply.To = Order.Commit, me.members
				me.timer = time.After(me.leaderHeartbeat)
				log.Printf("Leader: quorum have agreed request %v, I will commit and boardcast it\n", msg.LastLogKey)
			}
		}
		nextKey := me.logs.GetNext(msg.LastLogKey) // 如果leader还有数据需要同步，leader尝试发送下一条
		if nextKey.Term != -1 {
			req, err := me.logs.GetContentByKey(nextKey)
			if err != nil {
				return err
			}
			me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Msg{
				Type:             Order.AppendLog,
				From:             reply.From,
				To:               []int{msg.From},
				Term:             reply.Term,
				LastLogKey:       nextKey,
				SecondLastLogKey: msg.LastLogKey,
				Log:              req,
			}}
			log.Printf("Leader: %d accept my request %v, but %d's logs is not complete, send request %v\n",
				msg.From, msg.LastLogKey, msg.From, nextKey)
		}
	} else { // 如果follower不同意我的请求，说明数据未对齐，我需要获取它的最后一条日志，之后计算它的最后一条日志的前一条日志，将其发送出去
		reply.LastLogKey = me.logs.GetNext(msg.SecondLastLogKey) // 尝试发送
		log.Printf("Leader: %d refuse my request %v, his logs are not complete, which is %v, send request %v\n",
			msg.From, msg.LastLogKey, msg.SecondLastLogKey, reply.LastLogKey)
		if reply.LastLogKey.Term == -1 { // 没有下一条，报错，因为follower发送过来的second一定小于leader的last
			return errors.New("error: follower request wrong log")
		}
		reply.SecondLastLogKey = msg.SecondLastLogKey
		reply.Type, reply.To = Order.AppendLog, []int{msg.From} // 继续发送数据
		if req, err := me.logs.GetContentByKey(reply.LastLogKey); err != nil {
			return err
		} else {
			reply.Log = req
		}
	}
	if len(reply.To) != 0 { // 如果有需要发送，发送
		me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
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

func (l *Leader) processClient(msg Order.Msg, me *Me) error { // 处理一个客户端请求
	secondLastKey := me.logs.GetLast()                               // 获取我的最后一个日志
	lastLogKey := Log.LogKeyType{Term: me.meta.Term, Index: l.index} // 创建一个新的日志
	me.logs.Append(Log.Content{LogKey: lastLogKey, Log: msg.Log})    // 将日志添加到本地
	l.agree[lastLogKey] = map[int]bool{}                             // 为agree列表添加一笔请求
	l.client[lastLogKey] = msg.From                                  // 设置client
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Msg{
		Type:             Order.AppendLog,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		LastLogKey:       lastLogKey,
		SecondLastLogKey: secondLastKey,
		Log:              msg.Log,
	}}
	me.timer = time.After(me.leaderHeartbeat)
	l.index++ // 预index++
	log.Printf("Leader: reveive a client's request whose key: %v, log: %v, now I will broadcast it\n", lastLogKey, msg.Log)
	return nil
}

func (l *Leader) processTimeout(me *Me) error {
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Msg{
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

func (l *Leader) processExpansion(msg Order.Msg, me *Me) error {
	return nil
}

func (l *Leader) processExpansionReply(msg Order.Msg, me *Me) error {
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
