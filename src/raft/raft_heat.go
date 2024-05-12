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

	// 主从节点日志匹配不上时的从库日志索引
	ConflictIndex int
	ConflictTerm  int64
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
			LOG(rf.me, int(rf.currentTerm), DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, int(rf.currentTerm), DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		if !reply.Success {
			// 与从节点冲突时，一个一个任期的回退，这样在大量日志不对齐的情况下，很耗性能
			//idx := rf.nextIndex[peer] - 1
			//term := rf.log[idx].Term
			//for idx > 0 && rf.log[idx].Term == term {
			//	idx--
			//}
			//rf.nextIndex[peer] = idx + 1
			//LOG(rf.me, int(rf.CurrentTerm), DLog, "Log not matched in %d, Update next=%d",
			//	args.PrevLogIndex, rf.nextIndex[peer])
			//return

			preIndex := rf.nextIndex[peer]
			// 从节点给出冲突的索引及任期
			// 说明从节点日志太短
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstIndex := rf.log.firstFor(reply.ConflictTerm)
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}

			// 避免乱序
			// 匹配探测期比较长时，会有多个探测的 RPC，如果 RPC 结果乱序回来：
			// 一个先发出去的探测 RPC 后回来了，其中所携带的 ConfilictTerm 和 ConfilictIndex 就有可能造成 rf.next 的“反复横跳”。
			if rf.nextIndex[peer] > preIndex {
				rf.nextIndex[peer] = preIndex
			}

			// 仅仅是为了打日志
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIndex {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}

			LOG(rf.me, int(rf.currentTerm), DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevTerm, nextPrevIndex)
			LOG(rf.me, int(rf.currentTerm), DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityIndexLocked()
		// 第二个判断条件，是指只能本次任期的提交来间接提交之前任期的log
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, int(rf.currentTerm), DApply, "Leader update the commit index %d->%d",
				rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		//fmt.Println("startReplication")
		LOG(rf.me, int(rf.currentTerm), DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		if prevIdx < rf.log.snapLastIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIndex,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, int(rf.currentTerm), DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())
			go rf.installToPeer(peer, term, args)
			continue
		}

		prevTerm := rf.log.at(prevIdx).Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}

		go replicationToPeer(peer, args)
		LOG(rf.me, int(rf.currentTerm), DDebug, "-> S%d, Send log, Prev=[%d]T%d, Len()=%d",
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
	LOG(rf.me, int(rf.currentTerm), DDebug, "Match index after sort: %v, majority[%d]=%d",
		tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx] // min -> max
}
