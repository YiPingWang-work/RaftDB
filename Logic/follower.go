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
	log.Printf("Follower: leader %d's heartbeat\n", msg.From)
	if me.logs.GetLast().Less(msg.LastLogKey) { // 如果在心跳过程中，发现自己的日志不是leader的最新日志，则转到缺少日志处理，如果比较多在心跳的时候不管，等下一次同步新日志时处理
		log.Println("Follower: my logs are not complete")
		return f.processAppendLog(msg, me)
	}
	return nil
}

func (f *Follower) processAppendLog(msg Order.Msg, me *Me) error {
	reply := Order.Msg{
		Type:       Order.AppendLogReply,
		From:       me.meta.Id,
		To:         []int{msg.From},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if !me.logs.GetCommitted().Less(msg.LastLogKey) { // 如果收到希望处理的日志比自己已提交的日志要小，不处理
		return nil
	}
	// 剩下的日志都是希望处理的日志大于自己已提交日志，也就是secondLast一定大于等于自己已提交的日志
	if me.logs.GetLast().Greater(msg.SecondLastLogKey) { // 如果自己的日志比secondLast大，那么需要删除到自己日志的最后一条是secondLast
		if _, err := me.logs.Remove(msg.SecondLastLogKey); err != nil { // 如果报错，则和上面的"也就是secondLast一定大于等于自己已提交的日志"冲突
			return err
		}
		log.Printf("Follower: receive a less log %v from %d, remove logs until last log is %v\n",
			msg.LastLogKey, msg.From, me.logs.GetLast())
	}
	// 到这里，自己的最后一条日志一定比secondLast小或者等于它
	if me.logs.GetLast().Equals(msg.SecondLastLogKey) && msg.Type == Order.AppendLog { // 如果等于，并且日志中有信息，提交
		reply.Agree = true
		me.logs.Append(Log.Content{LogKey: msg.LastLogKey, Log: msg.Log})
		log.Printf("Follower: accept %d's request %v\n", msg.From, msg.LastLogKey)
	} else { // 如果没有或者msg中没有日志信息，则返回错误，提供自己反对提交msg.LastLogKey和自己的最后一条日志me.logs.GetLast
		reply.Agree, reply.SecondLastLogKey = false, me.logs.GetLast()
		log.Printf("Follower: refuse %d's request %v, my last log is %v\n", msg.From, msg.LastLogKey, me.logs.GetLast())
	}
	// 这里发出的自己的second一定是小于leader的最后一条日志的
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
	me.timer = time.After(me.followerTimeout)
	return nil
}

func (f *Follower) processAppendLogReply(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) processCommit(msg Order.Msg, me *Me) error {
	if !me.logs.GetCommitted().Less(msg.LastLogKey) { // 如果收到的提交申请没有自己的已提交日志大，直接返回
		return nil
	}
	previousCommitted := me.logs.Commit(msg.LastLogKey) // 直接提交之前的所有消息
	if previousCommitted == me.logs.GetCommitted() {    // 如果它的提交消息是很大的，就不能提交
		return nil
	}
	me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = me.logs.GetCommitted().Term, me.logs.GetCommitted().Index
	me.toBottomChan <- Order.Order{
		Type: Order.Store,
		Msg: Order.Msg{
			Agree:            false,
			LastLogKey:       me.logs.GetCommitted(),
			SecondLastLogKey: me.logs.GetNext(previousCommitted),
		}}
	if metaTmp, err := json.Marshal(*me.meta); err != nil {
		return err
	} else {
		me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(metaTmp)}}
	}
	me.timer = time.After(me.followerTimeout)
	log.Printf("Follower: commit logs whose key from %v to %v\n",
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
		reply.Agree, reply.SecondLastLogKey = false, me.logs.GetLast()
		log.Printf("Follower: refuse %d's vote, because vote: %d, myLastKey: %v, yourLastKey: %v\n",
			msg.From, f.voted, reply.SecondLastLogKey, msg.LastLogKey)
	} else {
		f.voted = msg.From
		reply.Agree = true
		log.Printf("Follower: agree %d's vote\n", msg.From)
	}
	me.toBottomChan <- Order.Order{
		Type: Order.NodeReply,
		Msg:  reply,
	}
	return nil
}

func (f *Follower) processVoteReply(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) processPreVote(msg Order.Msg, me *Me) error {
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Msg{
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

func (f *Follower) processClient(msg Order.Msg, me *Me) error {
	return errors.New("error: client --x-> follower")
}

func (f *Follower) processTimeout(me *Me) error {
	log.Println("Follower: timeout")
	return me.switchToCandidate()
}

func (f *Follower) processExpansion(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) processExpansionReply(msg Order.Msg, me *Me) error {
	return nil
}

func (f *Follower) ToString() string {
	return "==== FOLLOWER ====\n==== FOLLOWER ===="
}
