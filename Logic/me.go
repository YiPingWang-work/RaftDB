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
	receiveChan             <-chan Order.Order // 接收bottom消息的管道
	replyChan               chan<- Order.Order // 发送消息给bottom的管道
	logs                    *Log.Logs          // 日志指针
	leaderHeartbeat         time.Duration      // leader心跳间隔
	followerTimeout         time.Duration      // follower超时时间
	candidatePreVoteTimeout time.Duration      // candidate预选举超时
	candidateVoteTimeout    time.Duration      // candidate选举超时
}

type Role interface {
	init(me *Me) error
	processHeartbeat(msg Order.Msg, me *Me) error
	processRequest(msg Order.Msg, me *Me) error
	processRequestReply(msg Order.Msg, me *Me) error
	processCommit(msg Order.Msg, me *Me) error
	processVote(msg Order.Msg, me *Me) error
	processVoteReply(msg Order.Msg, me *Me) error
	processPreVote(msg Order.Msg, me *Me) error
	processPreVoteReply(msg Order.Msg, me *Me) error
	processClient(req Log.LogType, me *Me) error
	processTimeout(me *Me) error
	ToString() string
}

func (m *Me) Init(meta *Meta.Meta, logs *Log.Logs, receiveChan <-chan Order.Order, replyChan chan<- Order.Order) {
	m.meta, m.logs = meta, logs
	m.receiveChan, m.replyChan = receiveChan, replyChan
	m.members, m.quorum = make([]int, meta.Num), meta.Num/2
	for i := 0; i < meta.Num; i++ {
		m.members[i] = i
	}
	m.leaderHeartbeat = time.Duration(meta.LeaderHeartbeat) * time.Second
	m.followerTimeout = time.Duration(meta.FollowerTimeout) * time.Second
	m.candidateVoteTimeout = time.Duration(meta.CandidateVoteTimeout) * time.Second
	m.candidatePreVoteTimeout = time.Duration(meta.CandidatePreVoteTimeout) * time.Second
	if err := m.switchToFollower(m.meta.Term, false, Order.Msg{}); err != nil {
		log.Println(err)
	}
}

func (m *Me) Run() {
	for {
		select {
		case order, ok := <-m.receiveChan: // 获取receiveChan管道的命令，bottom会往里面发送数据
			if !ok {
				log.Println("Logic: Bye")
				return
			}
			if order.Type == Order.FromNode { // 如果是其它节点发送的数据
				if err := m.process(order.Msg); err != nil {
					log.Println(err)
				}
			}
			if order.Type == Order.FromClient { // 如果是客户端发送的数据
				if err := m.role.processClient(order.Msg.Log, m); err != nil {
					log.Println(err)
				}
			}
		case <-m.timer: // 计时器到期处理
			if err := m.role.processTimeout(m); err != nil {
				log.Println(err)
			}
		}
	}
}

func (m *Me) process(msg Order.Msg) error { // 所有状态处理其它节点消息的逻辑
	if m.meta.Term > msg.Term || m.meta.Id == msg.From { // 过期消息或者自己的消息不收
		return nil
	} else if m.meta.Term < msg.Term { // 如果自己的term小，则需要更新自己的任期
		return m.switchToFollower(msg.Term, true, msg)
	}
	switch msg.Type { // 处理不同类型的消息
	case Order.Heartbeat:
		return m.role.processHeartbeat(msg, m)
	case Order.Request:
		return m.role.processRequest(msg, m)
	case Order.RequestReply:
		return m.role.processRequestReply(msg, m)
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

func (m *Me) switchToFollower(term int, has bool, msg Order.Msg) error { // 切换为follower，如果还有余下的消息没处理按照follower逻辑处理这些消息
	log.Printf("==== switch to follower, my term is %d, has remain msg to process: %v ====\n", term, has)
	if m.meta.Term < term { // 可能需要修正自己的任期并持久化
		m.meta.Term = term
		if metaTmp, err := json.Marshal(*m.meta); err != nil {
			return err
		} else {
			m.replyChan <- Order.Order{Type: Order.Store, Msg: Order.Msg{Agree: true, Log: Log.LogType(string(metaTmp))}}
		}
	}
	m.role = &follower
	m.replyChan <- Order.Order{Type: Order.ClientLicense, Msg: Order.Msg{Agree: false}} // 关闭发送许可
	if err := m.role.init(m); err != nil {
		return err
	}
	if has {
		return m.process(msg)
	} else {
		return nil
	}
}

func (m *Me) switchToLeader() error { // 切换为leader
	log.Printf("==== switch to leader, my term is %d ====\n", m.meta.Term)
	m.role = &leader
	m.replyChan <- Order.Order{Type: Order.ClientLicense, Msg: Order.Msg{Agree: true}} // 开启发送许可
	return m.role.init(m)
}

func (m *Me) switchToCandidate() error { // 切换为candidate
	log.Printf("==== switch to candidate, my term is %d ====\n", m.meta.Term)
	m.role = &candidate
	return m.role.init(m)
}

func (m *Me) ToString() string { // 打印当前逻辑信息和日志信息
	return m.meta.ToString() + "\n" + m.role.ToString() + "\n" + m.logs.ToString()
}
