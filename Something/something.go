package Something

type Something struct {
	Id        int    // 标识消息发送方的Id
	NeedReply bool   // 是否需要回复
	Agree     bool   // 命令是否合法
	Content   string // 消息正文
}
