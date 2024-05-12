package raft

import (
	"fmt"
	"hhdb/labgob"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int64

	// 从下标1开始，下标0是snapshot的最后一个日志的任期和index
	snapshot []byte
	tailLog  []LogEntry
}

func NewLog(lastIndex int, lastTerm int64, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: lastIndex,
		snapLastTerm:  lastTerm,
		snapshot:      snapshot,
	}

	rl.tailLog = make([]LogEntry, len(entries)+1)
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term:         lastTerm,
		CommandValid: false,
		Command:      nil,
	})
	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIndex = lastIdx

	var lastTerm int64
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIndex)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return len(rl.tailLog) + rl.snapLastIndex
}

func (rl *RaftLog) idx(logicIndex int) int {
	// 已经做了snapshot的日志，是不能直接访问的
	if logicIndex < rl.snapLastIndex || logicIndex >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIndex, rl.snapLastIndex, rl.size()-1))
	}

	return logicIndex - rl.snapLastIndex
}

func (rl *RaftLog) at(index int) LogEntry {
	return rl.tailLog[rl.idx(index)]
}

func (rl *RaftLog) last() (index int, term int64) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIndex + i, rl.tailLog[i].Term
}

func (rl *RaftLog) firstFor(term int64) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}

	return rl.tailLog[rl.idx(startIdx):]
}

func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex)+1], entries...)
}

// string methods for debug
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIndex
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastIndex+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIndex+len(rl.tailLog)-1, prevTerm)
	return terms
}

func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	if index <= rl.snapLastIndex {
		return
	}

	idx := rl.idx(index)
	rl.snapshot = snapshot
	rl.snapLastIndex = index
	rl.snapLastTerm = rl.tailLog[idx].Term

	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIndex)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[rl.snapLastIndex+1:]...)
	rl.tailLog = newLog
}

func (rl *RaftLog) installSnapshot(index int, term int64, snapshot []byte) {
	rl.snapLastIndex = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	// make a new log array
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}