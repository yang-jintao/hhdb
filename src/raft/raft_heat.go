package raft

import (
	"sort"
	"time"
)

const (
	replicaInterval = 100
)

type LogEntry struct {
	Term         int64
	CommandValid bool
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int64
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) replicationTicker(term int64) {
	for !rf.killed() {
		if ok := rf.startReplication(term); !ok {
			return
		}

		time.Sleep(replicaInterval)
	}
}

func (rf *Raft) startReplication(term int64) bool {
	replicationToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		if !ok {
			//fmt.Println("startReplication")
			LOG(rf.me, int(rf.CurrentTerm), DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.CurrentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		if !reply.Success {
			idx := rf.nextIndex[peer] - 1
			term := rf.log[idx].Term
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, int(rf.CurrentTerm), DLog, "Log not matched in %d, Update next=%d",
				args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex {
			LOG(rf.me, int(rf.CurrentTerm), DApply, "Leader update the commit index %d->%d",
				rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		//fmt.Println("startReplication")
		LOG(rf.me, int(rf.CurrentTerm), DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.CurrentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term

		args := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      append([]LogEntry(nil), append(rf.log[prevIdx+1:])...),
			LeaderCommit: rf.commitIndex,
		}

		go replicationToPeer(peer, args)
		LOG(rf.me, int(rf.CurrentTerm), DDebug, "-> S%d, Send log, Prev=[%d]T%d, Len()=%d",
			peer, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	}

	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 获取大部分节点同步过来的日志索引，该索引应当被apply
func (rf *Raft) getMajorityIndexLocked() int {
	// TODO(spw): may could be avoid copying
	tmpIndexes := make([]int, len(rf.matchIndex))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(tmpIndexes) - 1) / 2
	LOG(rf.me, int(rf.CurrentTerm), DDebug, "Match index after sort: %v, majority[%d]=%d",
		tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx] // min -> max
}
