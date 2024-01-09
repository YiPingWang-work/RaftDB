package Logic

import (
	"RaftDB/Log"
	"RaftDB/Order"
	"RaftDB/Something"
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

/*
收到leader的心跳，重制定时器。
如果发现自己的LastLogKey比心跳中携带的leader的LastLogKey小，那么转到processLogAppend触发日志缺失处理。
这里如果自己的日志比leader大，不做处理，等到新消息到来时再删除。
*/

func (f *Follower) processHeartbeat(msg Order.Message, me *Me) error {
	me.timer = time.After(me.followerTimeout)
	log.Printf("Follower: leader %d's heartbeat\n", msg.From)
	if me.logs.GetLast().Less(msg.LastLogKey) {
		log.Println("Follower: my logs are not complete")
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
	if !me.logs.GetCommitted().Less(msg.LastLogKey) { // 如果收到希望处理的日志比自己已提交的日志要小，不处理
		return nil
	}
	if me.logs.GetLast().Greater(msg.SecondLastLogKey) {
		if contents, err := me.logs.Remove(msg.SecondLastLogKey); err != nil { // 如果报错，则和上面的"也就是secondLast一定大于等于自己已提交的日志"冲突
			return err
		} else {
			for _, v := range contents {
				me.toCrownChan <- Something.Something{NeedReply: false, Content: "!" + v.Log}
			}
		}
		log.Printf("Follower: receive a less log %v from %d, remove logs until last log is %v\n",
			msg.LastLogKey, msg.From, me.logs.GetLast())
	}
	if me.logs.GetLast().Equals(msg.SecondLastLogKey) && msg.Type == Order.AppendLog {
		reply.Agree = true
		me.logs.Append(Log.LogContent{Key: msg.LastLogKey, Log: msg.Log})
		me.toCrownChan <- Something.Something{NeedReply: false, Content: msg.Log}
		log.Printf("Follower: accept %d's request %v\n", msg.From, msg.LastLogKey)
	} else {
		reply.Agree, reply.SecondLastLogKey = false, me.logs.GetLast()
		log.Printf("Follower: refuse %d's request %v, my last log is %v\n", msg.From, msg.LastLogKey, me.logs.GetLast())
	}
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
	me.timer = time.After(me.followerTimeout)
	return nil
}

func (f *Follower) processAppendLogReply(msg Order.Message, me *Me) error {
	return nil
}

/*
follower将提交所有小于等于提交请求key的log。
*/

func (f *Follower) processCommit(msg Order.Message, me *Me) error {
	if !me.logs.GetCommitted().Less(msg.LastLogKey) {
		return nil
	}
	previousCommitted := me.logs.Commit(msg.LastLogKey)
	if previousCommitted == me.logs.GetCommitted() {
		return nil
	}
	me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = me.logs.GetCommitted().Term, me.logs.GetCommitted().Index
	if metaTmp, err := json.Marshal(*me.meta); err != nil {
		return err
	} else {
		me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Message{Agree: true, Log: string(metaTmp)}}
	}
	me.toBottomChan <- Order.Order{
		Type: Order.Store,
		Msg: Order.Message{
			Agree:            false,
			LastLogKey:       me.logs.GetCommitted(),
			SecondLastLogKey: me.logs.GetNext(previousCommitted),
		}}
	me.timer = time.After(me.followerTimeout)
	log.Printf("Follower: commit logs whose key from %v to %v\n",
		me.logs.GetNext(previousCommitted), me.logs.GetCommitted())
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
	if f.voted != -1 && f.voted != msg.From || me.logs.GetLast().Greater(msg.LastLogKey) {
		reply.Agree, reply.SecondLastLogKey = false, me.logs.GetLast()
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

func (f *Follower) processVoteReply(msg Order.Message, me *Me) error {
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

func (f *Follower) processPreVoteReply(msg Order.Message, me *Me) error {
	return nil
}

func (f *Follower) processFromClient(msg Order.Message, me *Me) error {
	if msg.Agree {
		return errors.New("error: follower can not do sync")
	}
	me.toCrownChan <- Something.Something{Id: msg.From, NeedReply: true, Content: msg.Log}
	return nil
}

func (f *Follower) processClientSync(msg Order.Message, me *Me) error {
	return errors.New("error: follower can not do sync")
}

func (f *Follower) processTimeout(me *Me) error {
	log.Println("Follower: timeout")
	return me.switchToCandidate()
}

func (f *Follower) processExpansion(msg Order.Message, me *Me) error {
	return nil
}

func (f *Follower) processExpansionReply(msg Order.Message, me *Me) error {
	return nil
}

func (f *Follower) ToString() string {
	return "==== FOLLOWER ====\n==== FOLLOWER ===="
}
