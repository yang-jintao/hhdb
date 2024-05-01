package raft

import (
	"fmt"
	"time"
)

const (
	replicaInterval = 100
)

type AppendEntriesArgs struct {
	Term     int64
	LeaderId int
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
			fmt.Println("startReplication")
			LOG(rf.me, int(rf.CurrentTerm), DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.CurrentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		fmt.Println("startReplication")
		LOG(rf.me, int(rf.CurrentTerm), DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.CurrentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.CurrentTerm,
			LeaderId: rf.me,
		}

		go replicationToPeer(peer, args)
	}

	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
