package raft

import (
	"bytes"
	"fmt"
	"hhdb/labgob"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VotedFor: %d, Log: [0: %d)", rf.currentTerm, rf.VotedFor, rf.log.size())
}

// 持久化存储接口
func (rf *Raft) persistLock() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 根据论文所述，持久化日志、任期、投票结果
	e.Encode(rf.currentTerm)
	e.Encode(rf.VotedFor)
	//e.Encode(rf.log)
	// 持久化日志，不包括snapshot，snapshot在Save()中持久化
	rf.log.persist(e)

	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.log.snapshot)
	LOG(rf.me, int(rf.currentTerm), DPersist, "Persist: %v", rf.persistString())
}

// 读持久化的数据接口
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	var currentTerm int64
	var votedFor int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, int(rf.currentTerm), DPersist, "Read currentTerm error: %v", err)
		return
	}
	rf.currentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, int(rf.currentTerm), DPersist, "Read votedFor error: %v", err)
		return
	}

	rf.VotedFor = votedFor

	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, int(rf.currentTerm), DPersist, "Read log error: %v", err)
		return
	}
	rf.log.snapshot = rf.persister.ReadSnapshot()

	// todo: 目的？
	if rf.log.snapLastIndex > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastIndex
		rf.lastApplied = rf.log.snapLastIndex
	}

	LOG(rf.me, int(rf.currentTerm), DPersist, "Read from persist: %v", rf.persistString())
}
