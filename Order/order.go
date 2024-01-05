package Order

import (
	"RaftDB/Log"
	"fmt"
)

const (
	Send OrderType = iota
	Store
	ClientLicense
	FromNode
	FromClient
	ClientReply
	NIL
)

type Order struct {
	Type OrderType // 命令种类
	Msg  Msg       //消息正文
}

type MsgType int

type OrderType int

var orderTypes []string = []string{
	"Send",
	"Store",
	"ClientLicense",
	"FromNode",
	"FromClient",
	"ClientReply",
}

const (
	Heartbeat MsgType = iota
	Request
	Commit
	RequestReply
	Vote
	VoteReply
	PreVote
	PreVoteReply
)

var msgTypes []string = []string{
	"Heartbeat",
	"Request",
	"Commit",
	"RequestReply",
	"Vote",
	"VoteReply",
	"PreVote",
	"PreVoteReply",
}

type Msg struct {
	Type             MsgType        `json:"type"`                // 消息类型
	From             int            `json:"from"`                // 消息来源
	To               []int          `json:"to"`                  // 消息去向
	Term             int            `json:"term"`                // 消息发送方的任期/客户端设置的超时微秒数
	Agree            bool           `json:"agree"`               // relay消息的回复/客户端消息确认/是否释放客户端应答权限/存储日志还是元数据（配置）
	LastLogKey       Log.LogKeyType `json:"last_log_key"`        // 要commit的消息/要请求的消息/存储日志的最后一条消息
	SecondLastLogKey Log.LogKeyType `json:"second_last_log_key"` // 要请求消息的前一条消息/存储日志的第一条消息
	Log              Log.LogType    `json:"log"`                 // 消息正文
}

func (o *Order) ToString() string {
	return fmt.Sprintf("{\n OrderType: %s\n Msg:{\n"+
		"  Type: %s\n  From: %d\n  To: %v\n  Term: %d\n  Agree: %v\n  LastLogKey: %v\n  SecondLastLogKey: %v\n  Log: %s\n }\n"+
		"}",
		orderTypes[o.Type], msgTypes[o.Msg.Type], o.Msg.From, o.Msg.To, o.Msg.Term,
		o.Msg.Agree, o.Msg.LastLogKey, o.Msg.SecondLastLogKey, o.Msg.Log)
}

func (m *Msg) ToString() string {
	return fmt.Sprintf("{\n Type: %s\n From: %d\n To: %v\n Term: %d\n Agree: %v\n LastLogKey: %v\n SecondLastLogKey: %v\n Log: %s\n}",
		msgTypes[m.Type], m.From, m.To, m.Term, m.Agree, m.LastLogKey, m.SecondLastLogKey, m.Log)
}
