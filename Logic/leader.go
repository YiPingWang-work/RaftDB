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

/*
初始化Leader，每当进行角色切换到额时候，必须调用此方法。
*/

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

func (l *Leader) processAppendLogReply(msg Order.Msg, me *Me) error {
	reply := Order.Msg{
		Type:       Order.Commit,
		From:       me.meta.Id,
		To:         []int{},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if me.logs.GetLast().Less(msg.LastLogKey) {
		/*
			如果follower回复的Key比自己的LastKey都大，错误。
		*/
		return errors.New("error: a follower has greater key")
	}
	if msg.Agree == true {
		if !me.logs.GetCommitted().Less(msg.LastLogKey) {
			/*
				如果回复的key自己已经提交，则不用参与计票，直接对其确认，发送给源follower。
			*/
			reply.To = []int{msg.From}
			log.Printf("Leader: %d should commit my committed log %v\n", msg.From, msg.LastLogKey)
		} else {
			/*
				计票，如果发现票数已经达到quorum，同时回复的key的Term为当前任期，则提交该日志，包括：更新元数据、内存更新日志、持久化日志到磁盘（上一次提交的日志到本条日志）。
				回复客户端数据提交成功。
				同时广播，让各个follower提交该日志。
			*/
			l.agree[msg.LastLogKey][msg.From] = true
			if len(l.agree[msg.LastLogKey]) >= me.quorum && me.meta.Term == msg.LastLogKey.Term {
				me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = msg.LastLogKey.Term, msg.LastLogKey.Index
				if metaTmp, err := json.Marshal(*me.meta); err != nil {
					return err
				} else {
					me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(metaTmp)}}
				}
				previousCommitted := me.logs.Commit(msg.LastLogKey)
				secondLastKey := me.logs.GetNext(previousCommitted)
				me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{
					Agree:            false,
					LastLogKey:       msg.LastLogKey,
					SecondLastLogKey: secondLastKey,
				}}
				for _, v := range me.logs.GetKeysByRange(secondLastKey, msg.LastLogKey) {
					me.toBottomChan <- Order.Order{Type: Order.ClientReply, Msg: Order.Msg{
						From:  me.meta.Id,
						To:    []int{l.client[v]},
						Agree: true,
					}}
					delete(l.client, v)
					delete(l.agree, v)
				}
				reply.To = me.members
				me.timer = time.After(me.leaderHeartbeat)
				log.Printf("Leader: quorum have agreed request %v, I will commit and boardcast it\n", msg.LastLogKey)
			}
		}
		/*
			如果这条日志不是leader最新的日志，则尝试发送这条日志的下一条给源follower
		*/
		nextKey := me.logs.GetNext(msg.LastLogKey)
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
	} else {
		/*
			如果不同意这条消息，发送follower回复的最新消息的下一条（follower的最新消息在msg.SecondLastLogKey中携带）
		*/
		reply.SecondLastLogKey = msg.SecondLastLogKey
		reply.LastLogKey = me.logs.GetNext(reply.SecondLastLogKey)
		if reply.LastLogKey.Term == -1 {
			return errors.New("error: follower request wrong log")
		}
		reply.Type, reply.To = Order.AppendLog, []int{msg.From}
		if req, err := me.logs.GetContentByKey(reply.LastLogKey); err != nil {
			return err
		} else {
			reply.Log = req
		}
		log.Printf("Leader: %d refuse my request %v, his logs are not complete, which is %v, send request %v\n",
			msg.From, msg.LastLogKey, msg.SecondLastLogKey, reply.LastLogKey)
	}
	if len(reply.To) != 0 {
		me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
	}
	return nil
}

func (l *Leader) processCommit(msg Order.Msg, me *Me) error {
	return errors.New("error: maybe two leaders")
}

func (l *Leader) processVote(msg Order.Msg, me *Me) error {
	return l.processTimeout(me)
}

func (l *Leader) processVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

func (l *Leader) processPreVote(msg Order.Msg, me *Me) error {
	return l.processTimeout(me)
}

func (l *Leader) processPreVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

/*
收到客户端的写请求，更新内存中的日志，更新自己的LastLogKey，初始化选举设置和客户端回复。
广播日志追加请求。
*/

func (l *Leader) processClient(msg Order.Msg, me *Me) error {
	secondLastKey := me.logs.GetLast()
	lastLogKey := Log.LogKeyType{Term: me.meta.Term, Index: l.index}
	me.logs.Append(Log.Content{LogKey: lastLogKey, Log: msg.Log})
	l.agree[lastLogKey] = map[int]bool{}
	l.client[lastLogKey] = msg.From
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
	l.index++
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
