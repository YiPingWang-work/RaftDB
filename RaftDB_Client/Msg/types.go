package Msg

type MsgType int

type LogType string

type LogKeyType struct {
	Term  int
	Index int
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

type Msg struct {
	Type             MsgType    `json:"type"`
	From             int        `json:"from"`
	To               []int      `json:"to"`
	Term             int        `json:"term"`
	Agree            bool       `json:"agree"`
	LastLogKey       LogKeyType `json:"last_log_key"`
	SecondLastLogKey LogKeyType `json:"second_last_log_key"`
	Log              LogType    `json:"log"`
}
