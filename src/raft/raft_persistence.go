package raft

import (
	"bytes"
	"fmt"
	"hhdb/labgob"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VotedFor: %d, Log: [0: %d)", rf.CurrentTerm, rf.VotedFor, len(rf.log))
}

// 持久化存储接口
func (rf *Raft) persistLock() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 根据论文所述，持久化日志、任期、投票结果
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)

	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
	LOG(rf.me, int(rf.CurrentTerm), DPersist, "Persist: %v", rf.persistString())
}

// 读持久化的数据接口
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	var currentTerm int64
	var votedFor int
	var log []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, int(rf.CurrentTerm), DPersist, "Read currentTerm error: %v", err)
		return
	}
	rf.CurrentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, int(rf.CurrentTerm), DPersist, "Read votedFor error: %v", err)
		return
	}

	rf.VotedFor = votedFor

	if err := d.Decode(&log); err != nil {
		LOG(rf.me, int(rf.CurrentTerm), DPersist, "Read log error: %v", err)
		return
	}

	rf.log = log

	LOG(rf.me, int(rf.CurrentTerm), DPersist, "Read from persist: %v", rf.persistString())
}
