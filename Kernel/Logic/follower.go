package Logic

import (
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Pipe/Order"
	"RaftDB/Kernel/Pipe/Something"
	"encoding/json"
	"errors"
	"log"
)

var follower Follower

type Follower struct {
	voted int
}

func (f *Follower) init(me *Me) error {
	me.timer.Reset(me.followerTimeout)
	f.voted = -1
	return nil
}

/*
收到leader的心跳，重制定时器。
如果发现自己的LastLogKey比心跳中携带的leader的LastLogKey小，那么转到processLogAppend触发日志缺失处理。
这里如果自己的日志比leader大，不做处理，等到新消息到来时再删除。
*/

func (f *Follower) processHeartbeat(msg Order.Message, me *Me) error {
	me.timer.Reset(me.followerTimeout)
	log.Printf("Follower: leader %d's heartbeat\n", msg.From)
	if me.logSet.GetLast().Less(msg.LastLogKey) {
		log.Println("Follower: my logSet are not complete")
		return f.processAppendLog(msg, me)
	}
	return nil
}

/*
追加日志申请，如果自己已提交的日志大于等于请求追加的日志，直接返回。
到这里自己的已提交的日志LogKey一定小于等于请求中的SecondLastLogKey且小于请求中的LastLogKey
如果自己日志对LastLogKey大于请求的SecondLastKey，那么删除日志直到自己的LastLogKey小于等于请求的SecondLastLogKey。
到这里，自己的最后一条日志一定比secondLastLogKey小或者等于它
之后如果日志中有信息（不是从Heartbeat得到的）且自己此时的LastLogKey就是请求的SecondLastLogKey，直接添加日志。并回复成功。
否则拒绝本次申请，同时在回复的SecondLastLogKey字段中给出自己的LastLogKey。
所有经过此函数发出的不同意的回复必须保证SecondLastLogKey小于发送过来的LastLogKey。
*/

func (f *Follower) processAppendLog(msg Order.Message, me *Me) error {
	reply := Order.Message{
		Type:       Order.AppendLogReply,
		From:       me.meta.Id,
		To:         []int{msg.From},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if !me.logSet.GetCommitted().Less(msg.LastLogKey) {
		return nil
	}
	if me.logSet.GetLast().Greater(msg.SecondLastLogKey) {
		if contents, err := me.logSet.Remove(msg.SecondLastLogKey); err != nil {
			panic("remove committed log")
		} else {
			for _, v := range contents {
				me.toCrownChan <- Something.Something{NeedReply: false, Content: "!" + v.V}
				if id, has := me.syncKeyIdMap[v.K]; has {
					me.syncIdMsgMap[id] = Order.Message{From: id, Log: "sync failed, rollback later"}
					me.syncFinishedChan <- id
					delete(me.syncKeyIdMap, v.K)
				}
			}
		}
		log.Printf("Follower: receive a less log %v from %d, remove logSet until last log is %v\n",
			msg.LastLogKey, msg.From, me.logSet.GetLast())
	}
	if me.logSet.GetLast().Equals(msg.SecondLastLogKey) && msg.Type == Order.AppendLog {
		reply.Agree = true
		me.logSet.Append(Log.Log{K: msg.LastLogKey, V: msg.Log})
		me.toCrownChan <- Something.Something{NeedReply: false, Content: msg.Log}
		log.Printf("Follower: accept %d's request %v\n", msg.From, msg.LastLogKey)
	} else {
		reply.Agree, reply.SecondLastLogKey = false, me.logSet.GetLast()
		log.Printf("Follower: refuse %d's request %v, my last log is %v\n", msg.From, msg.LastLogKey, me.logSet.GetLast())
	}
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
	me.timer.Reset(me.followerTimeout)
	return nil
}

func (f *Follower) processAppendLogReply(Order.Message, *Me) error {
	return nil
}

/*
follower将提交所有小于等于提交请求key的log。
*/

func (f *Follower) processCommit(msg Order.Message, me *Me) error {
	if !me.logSet.GetCommitted().Less(msg.LastLogKey) {
		return nil
	}
	previousCommitted := me.logSet.Commit(msg.LastLogKey)
	if me.logSet.GetCommitted().Equals(previousCommitted) {
		return nil
	}
	me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = me.logSet.GetCommitted().Term, me.logSet.GetCommitted().Index
	if metaTmp, err := json.Marshal(*me.meta); err != nil {
		return err
	} else {
		me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Message{Agree: true, Log: string(metaTmp)}}
	}
	k, _ := me.logSet.GetNext(previousCommitted)
	me.toBottomChan <- Order.Order{
		Type: Order.Store,
		Msg: Order.Message{
			Agree:            false,
			LastLogKey:       me.logSet.GetCommitted(),
			SecondLastLogKey: k,
		}}
	me.timer.Reset(me.followerTimeout)
	for _, v := range me.logSet.GetKsByRange(k, me.logSet.GetCommitted()) {
		if id, has := me.syncKeyIdMap[v]; has {
			me.syncFinishedChan <- id
			delete(me.syncKeyIdMap, v)
		}
	}
	log.Printf("Follower: commit logSet whose key from %v to %v\n",
		k, me.logSet.GetCommitted())
	return nil
}

/*
处理投票回复，如果follower在本轮（Term）已经投过票了或者自己的LastLogKey比Candidate大，那么他将拒绝，否则同意。
*/

func (f *Follower) processVote(msg Order.Message, me *Me) error {
	reply := Order.Message{
		Type: Order.VoteReply,
		From: me.meta.Id,
		To:   []int{msg.From},
		Term: me.meta.Term,
	}
	if f.voted != -1 && f.voted != msg.From || me.logSet.GetLast().Greater(msg.LastLogKey) {
		reply.Agree, reply.SecondLastLogKey = false, me.logSet.GetLast()
		log.Printf("Follower: refuse %d's vote, because vote: %d, myLastKey: %v, yourLastKey: %v\n",
			msg.From, f.voted, reply.SecondLastLogKey, msg.LastLogKey)
	} else {
		f.voted = msg.From
		reply.Agree = true
		log.Printf("Follower: agreeMap %d's vote\n", msg.From)
	}
	me.toBottomChan <- Order.Order{
		Type: Order.NodeReply,
		Msg:  reply,
	}
	return nil
}

func (f *Follower) processVoteReply(Order.Message, *Me) error {
	return nil
}

func (f *Follower) processPreVote(msg Order.Message, me *Me) error {
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Message{
		Type: Order.PreVoteReply,
		From: me.meta.Id,
		To:   []int{msg.From},
		Term: me.meta.Term,
	}}
	return nil
}

func (f *Follower) processPreVoteReply(Order.Message, *Me) error {
	return nil
}

func (f *Follower) processFromClient(msg Order.Message, me *Me) error {
	log.Printf("Follower: a msg from client: %v\n", msg)
	if msg.Agree {
		return errors.New("warning: follower refuses to sync")
	}
	me.toCrownChan <- Something.Something{Id: msg.From, NeedReply: true, NeedSync: false, Content: msg.Log}
	return nil
}

func (f *Follower) processClientSync(Order.Message, *Me) error {
	return errors.New("warning: follower can not do sync")
}

func (f *Follower) processTimeout(me *Me) error {
	log.Println("Follower: timeout")
	return me.switchToCandidate()
}

func (f *Follower) processExpansion(Order.Message, *Me) error {
	return nil
}

func (f *Follower) processExpansionReply(Order.Message, *Me) error {
	return nil
}

func (f *Follower) ToString() string {
	return "==== FOLLOWER ====\n==== FOLLOWER ===="
}
