package Logic

import (
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Meta"
	"RaftDB/Kernel/Pipe/Order"
	"RaftDB/Kernel/Pipe/Something"
	"encoding/json"
	"errors"
	"log"
	"time"
)

/*
重新改进一下，节点交互与节点和客户交互分离：【实现，现在实现的可能有缺陷】
1.接收一个客户端请求：
	1.1.如果是不需同步的请求，请求都会直接发往上层
	1.2.如果是需要同步的，同时me的状态是leader，则发往上层
	1.3.follower和candidate对于同步请求直接拦截，不发往上层，直接返回客户端错误
2.收到上层回复：
	2.1.如果是不需同步的请求，直接回复客户端，任务完成
	2.2.如果是需要同步的请求，此时还是leader，同步
	2.3.如果是需要同步的请求，此时是follower或者是candidate，拒绝，返回只同步了自己
3.在2中leader同步的时候me要记录一下这个要同步的logKey和客户端的映射，以及同步成功后的回执报文信息：
无论是leader还是follower完成一笔同步确认（当leader或follower可以提交的时候，candidate收到后会转变成follower执行），
都会查看这个提交的key和自己记录的映射中的信息，
需要维护发送给响应客户端的信息。
	3.1.如果commit的消息中有这条消息，则通知客户端完成。
	3.2.如果触发日志删除，则通知客户端失败（对于一个回滚操作，它一定会执行完，因为这个节点一个执行过该回滚操作对应的原操作，否则不会在日志中显示，
事实上，所有的需要同步的操作/写操作如果成功在本节点执行了都会记录内存日志）

这么做的意义是：将nodes之间的通讯和node和client之间的通讯解耦，对于不需同步的任务，所有的角色操作方式都是一致的，
对于需要同步的任务来说，本方案将其分离为顺序执行的两个过程，第一个过程就是一个不需要同步的过程，第一个过程结束后将第一个过程的结果进行记录。保存起来，
如果环境允许（属于leader状态）则进入第二个同步过程，第二个同步过程中可能进行了许多次环境更迭，但是只要最后产生了结果，就要返回，
因为针对客户端的这次消息，是唯一对应一个logKey的，此时对于me来说，只要它提交了logkey的报文，就说明这个任务已经同步了，完全可以正常返回，
如果这个logkey被忽略了，那么则返回错误，否则将一直阻塞直到产生一个结果。

关于永久阻塞：follower的逻辑是接收leader的心跳，只有在leader心跳term和index比自己大的时候才会触发申请或回滚，如果leader的任期期间迟迟没有
新消息到达，那么follower可能持续保留一个不正确的日志并不会修正。
可以通过leader成功选举后发送一条本阶段的提交消息来解决这一困境，让follower可以快速与leader保持同步

*/

type Me struct {
	meta                    *Meta.Meta                 // 元数据信息指针，用于状态变更，只允许Logic层修改元数据信息
	members                 []int                      // 维护的成员数量
	quorum                  int                        // 最小选举人数
	role                    Role                       // 当前角色
	timer                   *time.Timer                // 计时器
	fromBottomChan          <-chan Order.Order         // 接收bottom消息的管道
	toBottomChan            chan<- Order.Order         // 发送消息给bottom的管道
	fromCrownChan           <-chan Something.Something // 上层接口
	toCrownChan             chan<- Something.Something // 上层接口
	syncFinishedChan        chan int                   // 客户端同步处理完成通知
	syncIdMsgMap            map[int]Order.Message      // clientSyncMap是clientId -> msg的映射
	syncKeyIdMap            map[Log.Key]int            // clientSyncKeyIdMap是成功进入同步处理的logKsy -> clientId的映射
	logSet                  *Log.LogSet                // 日志指针
	leaderHeartbeat         time.Duration              // leader心跳间隔
	followerTimeout         time.Duration              // follower超时时间
	candidatePreVoteTimeout time.Duration              // candidate预选举超时
	candidateVoteTimeout    time.Duration              // candidate选举超时
}

/*
Role接口定义了处理各种消息的函数，Follower、Leader、Candidate角色类实现Role接口（状态机模型）。
在Me中会保存一个Role接口role，这个role代表自己的角色，me直接通过调用role的接口函数间接调用各个角色实现的函数，而不需要判断自己的角色是什么。
*/

type Role interface {
	init(me *Me) error
	processHeartbeat(msg Order.Message, me *Me) error
	processAppendLog(msg Order.Message, me *Me) error
	processAppendLogReply(msg Order.Message, me *Me) error
	processCommit(msg Order.Message, me *Me) error
	processVote(msg Order.Message, me *Me) error
	processVoteReply(msg Order.Message, me *Me) error
	processPreVote(msg Order.Message, me *Me) error
	processPreVoteReply(msg Order.Message, me *Me) error
	processExpansion(msg Order.Message, me *Me) error      // 节点变更，未实现
	processExpansionReply(msg Order.Message, me *Me) error // 节点变更回复，未实现
	processFromClient(msg Order.Message, me *Me) error
	processClientSync(msg Order.Message, me *Me) error
	processTimeout(me *Me) error
	ToString() string
}

/*
初始化，设置元数据信息，设置日志信息，设置超时时间，设置通讯管道（包括通向bottom端的和通向crown端的）
*/

func (m *Me) Init(meta *Meta.Meta, logSet *Log.LogSet,
	fromBottomChan <-chan Order.Order, toBottomChan chan<- Order.Order,
	fromCrownChan <-chan Something.Something, toCrownChan chan<- Something.Something) {
	m.meta, m.logSet = meta, logSet
	m.fromBottomChan, m.toBottomChan = fromBottomChan, toBottomChan
	m.fromCrownChan, m.toCrownChan = fromCrownChan, toCrownChan
	m.syncFinishedChan = make(chan int, 100000)
	m.syncIdMsgMap = map[int]Order.Message{}
	m.syncKeyIdMap = map[Log.Key]int{}
	m.members, m.quorum = make([]int, meta.Num), meta.Num/2
	m.timer = time.NewTimer(m.followerTimeout)
	for i := 0; i < meta.Num; i++ {
		m.members[i] = i
	}
	m.leaderHeartbeat = time.Duration(meta.LeaderHeartbeat) * time.Millisecond
	m.followerTimeout = time.Duration(meta.FollowerTimeout) * time.Millisecond
	m.candidateVoteTimeout = time.Duration(meta.CandidateVoteTimeout) * time.Millisecond
	m.candidatePreVoteTimeout = time.Duration(meta.CandidatePreVoteTimeout) * time.Millisecond
	if err := m.switchToFollower(m.meta.Term, false, Order.Message{}); err != nil {
		log.Println(err)
	}
}

/*
Logic层的主体函数，不断获取来自bottom的消息和定时器超时的消息，进行相应处理。
收到服务节点的消息后转到process函数。
收到客户端消息，说明自己是leader，直接调用processClient函数。
计时器到期后调用计时器到期处理函数。
在执行过程中发现通讯管道关闭，Panic返回。
*/

func (m *Me) Run() {
	for {
		select {
		case order, opened := <-m.fromBottomChan:
			if !opened {
				panic("bottom chan is closed")
				return
			}
			if order.Type == Order.FromNode {
				if err := m.processFromNode(order.Msg); err != nil {
					log.Println(err)
				}
			}
			if order.Type == Order.FromClient {
				if err := m.role.processFromClient(order.Msg, m); err != nil {
					/*
						如果处理客户端请求失败，立即回复客户端，并且这个请求被Logic层拦截，不会有后续处理。
						很可能client把同步请求发送给了follower
					*/
					log.Println(err)
					m.toBottomChan <- Order.Order{Type: Order.ClientReply,
						Msg: Order.Message{From: order.Msg.From, Log: "logic refuses to operate"}}
				}
			}
		case <-m.timer.C:
			if err := m.role.processTimeout(m); err != nil {
				log.Println(err)
			}
		case sth, opened := <-m.fromCrownChan:
			if !opened {
				panic("crown chan is closed")
			}
			id := sth.Id
			if !sth.Agree {
				/*
					如果Crown层返回不允许执行，则说明客户端的指令有问题,会把错误信息报告回客户端。
					Logic对其拦截，不会有后续处理，如果是同步请求，释放Logic层为其分配的资源。
				*/
				m.toBottomChan <- Order.Order{Type: Order.ClientReply,
					Msg: Order.Message{From: id, Log: sth.Content}}
				if _, has := m.syncIdMsgMap[id]; has {
					delete(m.syncIdMsgMap, id)
				}
				continue
			}
			if !sth.NeedSync {
				m.toBottomChan <- Order.Order{Type: Order.ClientReply, Msg: Order.Message{From: id, Log: sth.Content}}
				continue
			}
			if msg, has := m.syncIdMsgMap[id]; has {
				if err := m.role.processClientSync(msg, m); err != nil {
					log.Println(err)
					m.toBottomChan <- Order.Order{Type: Order.ClientReply,
						Msg: Order.Message{From: id, Log: "operated but logic refuses to sync, rollback later"}}
					delete(m.syncIdMsgMap, id)
				} else {
					m.syncIdMsgMap[id] = Order.Message{From: id, Log: sth.Content}
				}
			} else {
				panic("lose client msg")
			}
		case id, opened := <-m.syncFinishedChan:
			if !opened {
				panic("me.syncFinishedChan closed")
			}
			if msg, has := m.syncIdMsgMap[id]; has {
				m.toBottomChan <- Order.Order{Type: Order.ClientReply, Msg: msg}
				delete(m.syncIdMsgMap, id)
			} else {
				panic("lose client msg")
			}
		}
	}
}

/*
processFromNode方法是处理OrderType为FromNode所有命令中msg的共同逻辑。
首先会进行消息Term判断，如果发现收到了一则比自己Term大的消息，会转成follower之后继续处理这个消息。
如果发现消息的Term比自己小，说明是一个过期的消息，不予处理。
之后会根据消息的Type分类处理。
*/

func (m *Me) processFromNode(msg Order.Message) error {
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
*/

func (m *Me) switchToFollower(term int, has bool, msg Order.Message) error {
	log.Printf("==== switch to follower, my term is %d, has remain msg to process: %v ====\n", term, has)
	if m.meta.Term < term {
		m.meta.Term = term
		if metaTmp, err := json.Marshal(*m.meta); err != nil {
			return err
		} else {
			m.toBottomChan <- Order.Order{Type: Order.Store, Msg: Order.Message{Agree: true, Log: string(metaTmp)}}
		}
	}
	m.role = &follower
	if err := m.role.init(m); err != nil {
		return err
	}
	if has {
		return m.processFromNode(msg)
	}
	return nil
}

/*
切换为leader。
*/

func (m *Me) switchToLeader() error {
	log.Printf("==== switch to leader, my term is %d ====\n", m.meta.Term)
	m.role = &leader
	return m.role.init(m)
}

/*
切换为candidate。
*/

func (m *Me) switchToCandidate() error {
	log.Printf("==== switch to candidate, my term is %d ====\n", m.meta.Term)
	m.role = &candidate
	return m.role.init(m)
}

func (m *Me) ToString() string {
	return m.meta.ToString() + "\n" + m.role.ToString()
}
