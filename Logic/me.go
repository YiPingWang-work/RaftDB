package Logic

import (
	"RaftDB/Log"
	"RaftDB/Meta"
	"RaftDB/Order"
	"encoding/json"
	"errors"
	"log"
	"time"
)

type Me struct {
	meta                    *Meta.Meta         // 元数据信息指针，用于状态变更，只允许Logic层修改元数据信息
	members                 []int              // 维护的成员数量
	quorum                  int                // 最小选举人数
	role                    Role               // 当前角色
	timer                   <-chan time.Time   // 计时器
	fromBottomChan          <-chan Order.Order // 接收bottom消息的管道
	toBottomChan            chan<- Order.Order // 发送消息给bottom的管道
	fromCrownChan           <-chan interface{} // 上层接口
	ToCrownChan             chan<- interface{} // 上层接口
	logs                    *Log.Logs          // 日志指针
	leaderHeartbeat         time.Duration      // leader心跳间隔
	followerTimeout         time.Duration      // follower超时时间
	candidatePreVoteTimeout time.Duration      // candidate预选举超时
	candidateVoteTimeout    time.Duration      // candidate选举超时
}

/*
Role接口定义了处理各种消息的函数，Follower、Leader、Candidate角色类实现Role接口。
在Me中会保存一个Role接口role，这个role代表自己的角色，me直接通过调用role的接口函数间接调用各个角色实现的函数，而不需要判断自己的角色是什么。
*/

type Role interface {
	init(me *Me) error
	processHeartbeat(msg Order.Msg, me *Me) error
	processAppendLog(msg Order.Msg, me *Me) error
	processAppendLogReply(msg Order.Msg, me *Me) error
	processCommit(msg Order.Msg, me *Me) error
	processVote(msg Order.Msg, me *Me) error
	processVoteReply(msg Order.Msg, me *Me) error
	processPreVote(msg Order.Msg, me *Me) error
	processPreVoteReply(msg Order.Msg, me *Me) error
	processExpansion(msg Order.Msg, me *Me) error      // 节点变更，未实现
	processExpansionReply(msg Order.Msg, me *Me) error // 节点变更回复，未实现
	processClient(msg Order.Msg, me *Me) error
	processTimeout(me *Me) error
	ToString() string
}

/*
初始化，设置元数据信息，设置日志信息，设置超时时间，设置通讯管道
*/

func (m *Me) Init(meta *Meta.Meta, logs *Log.Logs,
	fromBottomChan <-chan Order.Order, toBottomChan chan<- Order.Order, fromCrownChan <-chan interface{}, toCrownChan chan<- interface{}) {
	m.meta, m.logs = meta, logs
	m.fromBottomChan, m.toBottomChan = fromBottomChan, toBottomChan
	m.fromCrownChan, m.ToCrownChan = fromCrownChan, toCrownChan
	m.members, m.quorum = make([]int, meta.Num), meta.Num/2
	for i := 0; i < meta.Num; i++ {
		m.members[i] = i
	}
	m.leaderHeartbeat = time.Duration(meta.LeaderHeartbeat) * time.Millisecond
	m.followerTimeout = time.Duration(meta.FollowerTimeout) * time.Millisecond
	m.candidateVoteTimeout = time.Duration(meta.CandidateVoteTimeout) * time.Millisecond
	m.candidatePreVoteTimeout = time.Duration(meta.CandidatePreVoteTimeout) * time.Millisecond
	if err := m.switchToFollower(m.meta.Term, false, Order.Msg{}); err != nil {
		log.Println(err)
	}
}

/*
Logic层的主体函数，不断获取来自bottom的消息和定时器超时的消息，进行相应处理。
收到服务节点的消息后转到process函数。
收到客户端消息，说明自己是leader，直接调用processClient函数。
计时器到期后调用计时器到期处理函数。
*/

func (m *Me) Run() {
	for {
		select {
		case order, ok := <-m.fromBottomChan:
			if !ok {
				log.Println("Logic: Bye")
				return
			}
			if order.Type == Order.FromNode {
				if err := m.process(order.Msg); err != nil {
					log.Println(err)
				}
			}
			if order.Type == Order.FromClient {
				if err := m.role.processClient(order.Msg, m); err != nil {
					log.Println(err)
				}
			}
		case <-m.timer:
			if err := m.role.processTimeout(m); err != nil {
				log.Println(err)
			}
		}
	}
}

/*
process方法是处理OrderType为FromNode所有命令中msg的共同逻辑。
首先会进行消息Term判断，如果发现收到了一则比自己Term大的消息，会转成follower之后继续处理这个消息。
如果发现消息的Term比自己小，说明是一个过期的消息，不予处理。
之后会根据消息的Type分类处理。
*/

func (m *Me) process(msg Order.Msg) error {
	if m.meta.Term > msg.Term || m.meta.Id == msg.From {
		return nil
	} else if m.meta.Term < msg.Term {
		return m.switchToFollower(msg.Term, true, msg)
	}
	switch msg.Type {
	case Order.Heartbeat:
		return m.role.processHeartbeat(msg, m)
	case Order.AppendLog:
		return m.role.processAppendLog(msg, m)
	case Order.AppendLogReply:
		return m.role.processAppendLogReply(msg, m)
	case Order.Commit:
		return m.role.processCommit(msg, m)
	case Order.Vote:
		return m.role.processVote(msg, m)
	case Order.VoteReply:
		return m.role.processVoteReply(msg, m)
	case Order.PreVote:
		return m.role.processPreVote(msg, m)
	case Order.PreVoteReply:
		return m.role.processPreVoteReply(msg, m)
	default:
		return errors.New("error: illegal msg type")
	}
}

/*
切换为follower，如果还有余下的消息没处理按照follower逻辑处理这些消息。
当切换为Follower的时候，会关闭客户端权限，也就是通知bottom禁止客户端连接。
*/

func (m *Me) switchToFollower(term int, has bool, msg Order.Msg) error {
	log.Printf("==== switch to follower, my term is %d, has remain msg to process: %v ====\n", term, has)
	if m.meta.Term < term {
		m.meta.Term = term
		if metaTmp, err := json.Marshal(*m.meta); err != nil {
			return err
		} else {
			m.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(metaTmp)}}
		}
	}
	m.role = &follower
	m.toBottomChan <- Order.Order{Type: Order.ClientLicense, Msg: Order.Msg{Agree: false}}
	if err := m.role.init(m); err != nil {
		return err
	}
	if has {
		return m.process(msg)
	} else {
		return nil
	}
}

/*
切换为leader。
当切换为leader的时候会开启客户端权限，也就是通知bottom可以接受客户端的连接请求。
*/

func (m *Me) switchToLeader() error {
	log.Printf("==== switch to leader, my term is %d ====\n", m.meta.Term)
	m.role = &leader
	m.toBottomChan <- Order.Order{Type: Order.ClientLicense, Msg: Order.Msg{Agree: true}} // 开启发送许可
	return m.role.init(m)
}

/*
切换为candidate
*/

func (m *Me) switchToCandidate() error {
	log.Printf("==== switch to candidate, my term is %d ====\n", m.meta.Term)
	m.role = &candidate
	return m.role.init(m)
}

func (m *Me) ToString() string {
	return m.meta.ToString() + "\n" + m.role.ToString()
}
