package Logic

import (
	"RaftDB/Log"
	"RaftDB/Order"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"
)

var candidate Candidate

type Candidate struct {
	agree map[int]bool
	state int // 0：预选举，1：预选举结束，第一次选举，2：选举结束，没有结果
}

func (c *Candidate) init(me *Me) error {
	c.agree = map[int]bool{}
	c.state = 0
	return c.processTimeout(me)
}

func (c *Candidate) processHeartbeat(msg Order.Msg, me *Me) error { // 收到同级心跳，直接转化为follower
	return me.switchToFollower(msg.Term, true, msg)
}

func (c *Candidate) processAppendLog(msg Order.Msg, me *Me) error { // 收到同级日志请求，直接转化为follower
	return me.switchToFollower(msg.Term, true, msg)
}

func (c *Candidate) processCommit(msg Order.Msg, me *Me) error { // 收到同级日志确认，直接转化为follower
	return me.switchToFollower(msg.Term, true, msg)
}

func (c *Candidate) processAppendLogReply(msg Order.Msg, me *Me) error {
	return nil
}

func (c *Candidate) processVote(msg Order.Msg, me *Me) error { // 候选人不给任何同级的其他人投票
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Msg{
		Type:  Order.VoteReply,
		From:  me.meta.Id,
		To:    []int{msg.From},
		Term:  me.meta.Term,
		Agree: false,
	}}
	log.Printf("Candidate: refuse %d's vote\n", msg.From)
	return nil
}

func (c *Candidate) processVoteReply(msg Order.Msg, me *Me) error {
	log.Printf("Candidate: %d agree my vote: %v\n", msg.From, msg.Agree)
	agreeNum := 0
	disagreeNum := 0
	c.agree[msg.From] = msg.Agree
	if len(c.agree) >= me.quorum { // 统计同意的人数
		for _, v := range c.agree {
			if v {
				agreeNum++
			} else {
				disagreeNum++
			}
		}
		if agreeNum >= me.quorum {
			return me.switchToLeader() // 同意的人数多
		} else if disagreeNum >= me.quorum {
			return me.switchToFollower(msg.Term, true, msg) // 不同意的人数多
		}
	}
	return nil
}

func (c *Candidate) processPreVote(msg Order.Msg, me *Me) error {
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: Order.Msg{
		Type: Order.PreVoteReply,
		From: me.meta.Id,
		To:   []int{msg.From},
		Term: me.meta.Term,
	}}
	return nil
}

func (c *Candidate) processPreVoteReply(msg Order.Msg, me *Me) error {
	if c.state == 0 { // 如果现在处于预选举
		c.agree[msg.From] = true
		if len(c.agree) >= me.quorum {
			c.agree = map[int]bool{}
			c.state = 1
			log.Println("Candidate: begin vote after a random time")
			me.timer = time.After(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}
	return nil
}

func (c *Candidate) processClient(msg Order.Msg, me *Me) error {
	return errors.New("error: client --x-> candidate")
}

func (c *Candidate) processTimeout(me *Me) error {
	log.Printf("Candidate: timeout, state: %v\n", c.state)
	reply := Order.Msg{
		From:       me.meta.Id,
		To:         me.members,
		LastLogKey: me.logs.GetLast(),
	}
	if c.state == 0 { // 如果还在预选举
		reply.Type = Order.PreVote
	} else if c.state == 1 { // 正式选举
		me.meta.Term++ // 更新任期
		c.state = 2    // 更新状态到正在选举中
		if metaTmp, err := json.Marshal(*me.meta); err != nil {
			return err
		} else {
			me.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(string(metaTmp))}}
		}
		reply.Type = Order.Vote
		log.Printf("Candidate: voting ... , my term is %d\n", me.meta.Term)
	} else { // 已经经过一轮选举了但是没有结果
		c.state = 0 // 重新回到预选举
		reply.Type = Order.PreVote
	}
	reply.Term = me.meta.Term
	me.toBottomChan <- Order.Order{Type: Order.NodeReply, Msg: reply}
	me.timer = time.After(me.candidatePreVoteTimeout)
	return nil
}

func (c *Candidate) processExpansion(msg Order.Msg, me *Me) error {
	return nil
}

func (c *Candidate) processExpansionReply(msg Order.Msg, me *Me) error {
	return nil
}

func (c *Candidate) ToString() string {
	res := fmt.Sprintf("==== CANDIDATE ====\nstate: %v\nagree:\n", c.state)
	for k, v := range c.agree {
		res += fmt.Sprintf("%d:%v ", k, v)
	}
	return res + "\n==== CANDIDATE ===="
}
