package Logic

import (
	"RaftDB/Log"
	"RaftDB/Order"
	"encoding/json"
	"errors"
	"log"
	"time"
)

var follower Follower

type Follower struct {
	voted int
}

func (f *Follower) init(me *Me) error {
	me.timer, f.voted = time.After(me.followerTimeout), -1
	return nil
}

func (f *Follower) processHeartbeat(msg Order.Msg, me *Me) error {
	me.timer = time.After(me.followerTimeout)
	if me.logs.GetLast().Less(msg.LastLogKey) { // 如果在心跳过程中，发现自己的日志小于leader的日志，发送一个缺少消息，使用-1 -1 默认让leader发送自己最大的消息
		log.Println("Follower: my log is not complete")
		me.replyChan <- Order.Order{
			Type: Order.Send,
			Msg: Order.Msg{
				Type:       Order.RequestReply,
				From:       me.meta.Id,
				To:         []int{msg.From},
				Term:       me.meta.Term,
				Agree:      false,
				LastLogKey: Log.LogKeyType{Term: -1, Index: -1},
			}}
	}
	return nil
}

func (f *Follower) processRequest(msg Order.Msg, me *Me) error {
	reply := Order.Msg{
		Type:       Order.RequestReply,
		From:       me.meta.Id,
		To:         []int{msg.From},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if me.logs.GetLast().Greater(msg.SecondLastLogKey) { // leader发送一个比较小的消息，此时需要删除
		if _, err := me.logs.Remove(msg.SecondLastLogKey); err != nil {
			return err
		}
		log.Printf("Follower: receive a less key %v from %d\n, remove until last key is %v\n",
			msg.From, msg.SecondLastLogKey, me.logs.GetLast())
	}
	if me.logs.GetLast().Equals(msg.SecondLastLogKey) { // 同意追加消息
		reply.Agree = true
		me.logs.Append(Log.Content{LogKey: msg.LastLogKey, Log: msg.Log})
		log.Printf("Follower: accept %d's request %v\n", msg.From, msg.LastLogKey)
	} else { // 还有别的消息没有到达，等待补全
		reply.Agree = false
		// reply.SecondLastLogKey = me.logs.GetLast() // 自己的最后一条消息位置信息放在SecondLastLogKey字段中，暂时没有优化
		log.Printf("Follower: refuse %d's request %v, my last key is %v\n", msg.From, msg.LastLogKey, me.logs.GetLast())
	}
	me.replyChan <- Order.Order{Type: Order.Send, Msg: reply}
	me.timer = time.After(me.followerTimeout)
	return nil
}

func (f *Follower) processRequestReply(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) processCommit(msg Order.Msg, me *Me) error {
	if !me.logs.GetCommitted().Less(msg.LastLogKey) { // 如果收到的提交申请没有自己的已提交日志大，直接返回
		return nil
	}
	previousCommitted := me.logs.Commit(msg.LastLogKey) // 直接提交之前的所有消息
	me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = me.logs.GetCommitted().Term, me.logs.GetCommitted().Index
	me.replyChan <- Order.Order{
		Type: Order.Store,
		Msg: Order.Msg{
			Agree:            false,
			LastLogKey:       me.logs.GetCommitted(),
			SecondLastLogKey: me.logs.GetNext(previousCommitted),
		}}
	if metaTmp, err := json.Marshal(*me.meta); err != nil {
		return err
	} else {
		me.replyChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(string(metaTmp))}}
	}
	me.timer = time.After(me.followerTimeout)
	log.Printf("Follower: commit logs with key from %v to %v\n",
		me.logs.GetNext(previousCommitted), me.logs.GetCommitted())
	return nil
}

func (f *Follower) processVote(msg Order.Msg, me *Me) error {
	reply := Order.Msg{
		Type: Order.VoteReply,
		From: me.meta.Id,
		To:   []int{msg.From},
		Term: me.meta.Term,
	}
	if f.voted != -1 && f.voted != msg.From || me.logs.GetLast().Greater(msg.LastLogKey) { // 如果已经投过票了或者自己的最后一条日志数比它的大，不同意
		reply.Agree = false
		log.Printf("Follower: refuse %d's vote, because vote: %d, myLastKey: %v, yourLastKey: %v\n",
			msg.From, f.voted, me.logs.GetLast(), msg.LastLogKey)
	} else {
		f.voted = msg.From
		reply.Agree = true
		log.Printf("Follower: agree %d's vote\n", msg.From)
	}
	me.replyChan <- Order.Order{
		Type: Order.Send,
		Msg:  reply,
	}
	return nil
}

func (f *Follower) processVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) processPreVote(msg Order.Msg, me *Me) error {
	me.replyChan <- Order.Order{Type: Order.Send, Msg: Order.Msg{
		Type: Order.PreVoteReply,
		From: me.meta.Id,
		To:   []int{msg.From},
		Term: me.meta.Term,
	}}
	return nil
}

func (f *Follower) processPreVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) processClient(req Log.LogType, me *Me) error {
	return errors.New("error: client --x-> follower")
}

func (f *Follower) processTimeout(me *Me) error {
	log.Println("Follower: timeout")
	return me.switchToCandidate()
}

func (f *Follower) ToString() string {
	return "==== FOLLOWER ====\n==== FOLLOWER ===="
}
