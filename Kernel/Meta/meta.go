package Meta

import "fmt"

type Meta struct {
	Id                      int      `json:"id"`
	Num                     int      `json:"num"`
	Term                    int      `json:"term"`
	CommittedKeyTerm        int      `json:"ckt"`
	CommittedKeyIndex       int      `json:"cki"`
	Dns                     []string `json:"dns"`
	LeaderHeartbeat         int      `json:"leaderHeartbeat"`
	FollowerTimeout         int      `json:"followerTimeout"`
	CandidatePreVoteTimeout int      `json:"candidatePreVoteTimeout"`
	CandidateVoteTimeout    int      `json:"candidateVoteTimeout"`
}

func (m *Meta) ToString() string {
	return fmt.Sprintf("==== meta ====\nid: %d\nnum of members: %d\nterm: %d\ncommittedKey: %d %d\ndns %v\n==== meta ====",
		m.Id, m.Num, m.Term, m.CommittedKeyTerm, m.CommittedKeyIndex, m.Dns[0:m.Num])
}
