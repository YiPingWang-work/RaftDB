package Something

type Something struct {
	Id        int    // 标识消息Id，全局唯一，crown禁止修改
	NeedReply bool   // 是否需要回复，crown禁止修改
	NeedSync  bool   // 需要同步，crown禁止修改
	Agree     bool   // 命令是否合法，如果合法且有同步任务需要继续执行，crown必须修改
	Content   string // 消息正文，crown接受信息并在这里给出回复
}
