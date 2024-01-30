package Logic

import (
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Pipe/Order"
	"RaftDB/Kernel/Pipe/Something"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

/*
如果发现当前集群出现两个及其以上的leader，Panic退出，因为会造成数据不一致。
*/

var leader Leader

type Leader struct {
	agreeMap map[Log.Key]fc // 对于哪条记录，同意的follower集合和这个消息来自哪个client
	index    int            // 当前日志的index
}

type fc struct {
	followers map[int]bool
}

/*
初始化Leader，每当进行角色切换到额时候，必须调用此方法。
*/

func (l *Leader) init(me *Me) error {
	l.agreeMap, l.index = map[Log.Key]fc{}, 0
	return l.processTimeout(me)
}

func (l *Leader) processHeartbeat(Order.Message, *Me) error {
	panic("maybe two leaders")
}

func (l *Leader) processAppendLog(Order.Message, *Me) error {
	panic("maybe two leaders")
}

func (l *Leader) processAppendLogReply(msg Order.Message, me *Me) error {
	reply := Order.Message{
		Type:       Order.Commit,
		From:       me.meta.Id,
		To:         []int{},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if me.logSet.GetLast().Less(msg.LastLogKey) {
		/*
			如果follower回复的Key比自己的LastKey都大，错误。
		*/
		return errors.New("error: a follower has greater key")
	}
	if msg.Agree == true {
		if !me.logSet.GetCommitted().Less(msg.LastLogKey) {
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
			l.agreeMap[msg.LastLogKey].followers[msg.From] = true
			if len(l.agreeMap[msg.LastLogKey].followers) >= me.quorum && me.meta.Term == msg.LastLogKey.Term {
				me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = msg.LastLogKey.Term, msg.LastLogKey.Index
				if metaTmp, err := json.Marshal(*me.meta); err != nil {
					return err
				} else {
					me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Message{Agree: true, Log: string(metaTmp)}}
				}
				previousCommitted := me.logSet.Commit(msg.LastLogKey)
				secondLastKey, _ := me.logSet.GetNext(previousCommitted)
				me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Message{
					Agree:            false,
					LastLogKey:       msg.LastLogKey,
					SecondLastLogKey: secondLastKey,
				}}
				for _, v := range me.logSet.GetKsByRange(secondLastKey, msg.LastLogKey) {
					if id, has := me.clientSyncKeyIdMap[v]; has {
						me.clientSyncFinishedChan <- id
						delete(me.clientSyncKeyIdMap, v)
					}
					if _, has := l.agreeMap[v]; has {
						delete(l.agreeMap, v)
					}
				}
				reply.To = me.members
				me.timer = time.After(me.leaderHeartbeat)
				log.Printf("Leader: quorum have agreed request %v, I will commit and boardcast it\n", msg.LastLogKey)
			}
		}
		/*
			如果这条日志不是leader最新的日志，则尝试发送这条日志的下一条给源follower
		*/
		nextKey, _ := me.logSet.GetNext(msg.LastLogKey)
		if nextKey.Term != -1 {
			req, err := me.logSet.GetVByK(nextKey)
			if err != nil {
				return err
			}
			me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Message{
				Type:             Order.AppendLog,
				From:             reply.From,
				To:               []int{msg.From},
				Term:             reply.Term,
				LastLogKey:       nextKey,
				SecondLastLogKey: msg.LastLogKey,
				Log:              req,
			}}
			log.Printf("Leader: %d accept my request %v, but %d's logSet is not complete, send request %v\n",
				msg.From, msg.LastLogKey, msg.From, nextKey)
		}
	} else {
		/*
			如果不同意这条消息，发送follower回复的最新消息的下一条（follower的最新消息在msg.SecondLastLogKey中携带）
		*/
		var err error
		reply.SecondLastLogKey = msg.SecondLastLogKey
		reply.LastLogKey, err = me.logSet.GetNext(reply.SecondLastLogKey)
		if err != nil { // 如果无法获得返回值的key，也就是这是一个废弃的key，那么leader将尝试自己的上一个key
			reply.LastLogKey, _ = me.logSet.GetPrevious(msg.LastLogKey)
			reply.SecondLastLogKey, _ = me.logSet.GetPrevious(reply.LastLogKey)
		}
		reply.Type, reply.To = Order.AppendLog, []int{msg.From}
		if req, err := me.logSet.GetVByK(reply.LastLogKey); err != nil {
			return err
		} else {
			reply.Log = req
		}
		log.Printf("Leader: %d refuse my request %v, his logSet are not complete, which is %v, send request %v\n",
			msg.From, msg.LastLogKey, msg.SecondLastLogKey, reply.LastLogKey)
	}
	if len(reply.To) != 0 {
		me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
	}
	return nil
}

func (l *Leader) processCommit(Order.Message, *Me) error {
	panic("maybe two leaders")
}

func (l *Leader) processVote(_ Order.Message, me *Me) error {
	return l.processTimeout(me)
}

func (l *Leader) processVoteReply(Order.Message, *Me) error {
	return nil
}

func (l *Leader) processPreVote(_ Order.Message, me *Me) error {
	return l.processTimeout(me)
}

func (l *Leader) processPreVoteReply(Order.Message, *Me) error {
	return nil
}

func (l *Leader) processFromClient(msg Order.Message, me *Me) error {
	log.Printf("Leader: a msg from client: %v\n", msg)
	if msg.Agree {
		me.clientSyncIdMsgMap[msg.From] = msg
	}
	me.toCrownChan <- Something.Something{Id: msg.From, NeedReply: true, NeedSync: msg.Agree, Content: msg.Log}
	return nil
}

func (l *Leader) processClientSync(msg Order.Message, me *Me) error {
	secondLastKey := me.logSet.GetLast()
	lastLogKey := Log.Key{Term: me.meta.Term, Index: l.index}
	me.clientSyncKeyIdMap[lastLogKey] = msg.From
	me.logSet.Append(Log.Log{K: lastLogKey, V: msg.Log})
	l.agreeMap[lastLogKey] = fc{followers: map[int]bool{}}
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Message{
		Type:             Order.AppendLog,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		Agree:            false,
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
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Message{
		Type:             Order.Heartbeat,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		LastLogKey:       me.logSet.GetLast(),
		SecondLastLogKey: me.logSet.GetSecondLast(),
	}}
	me.timer = time.After(me.leaderHeartbeat)
	log.Println("Leader: timeout")
	return nil
}

func (l *Leader) processExpansion(Order.Message, *Me) error {
	return nil
}

func (l *Leader) processExpansionReply(Order.Message, *Me) error {
	return nil
}

func (l *Leader) ToString() string {
	res := fmt.Sprintf("==== LEADER ====\nindex: %d\nagreedReply:\n", l.index)
	for k, v := range l.agreeMap {
		s := fmt.Sprintf("	key: {%d %d} -> ", k.Term, k.Index)
		for k2 := range v.followers {
			s += fmt.Sprintf("%d ", k2)
		}
		res += s + "\n"
	}
	return res + "==LEADER=="
}
