package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > rf.commitIndex {
		LOG(rf.me, int(rf.currentTerm), DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIndex {
		LOG(rf.me, int(rf.currentTerm), DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIndex)
		return
	}

	rf.log.doSnapshot(index, snapshot)
	rf.persistLock()
}

type InstallSnapshotArgs struct {
	Term     int64
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int64

	Snapshot []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int64
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// follower接受installSnapshot请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, int(rf.currentTerm), DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, int(rf.currentTerm), DSnap, "<- S%d, Reject Snap, Higher Term: T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term >= rf.currentTerm { // = handle the case when the peer is candidate
		rf.becomeFollower(args.Term)
	}

	// check if there is already a snapshot contains the one in the RPC
	if rf.log.snapLastIndex >= args.LastIncludedIndex {
		LOG(rf.me, int(rf.currentTerm), DSnap, "<- S%d, Reject Snap, Already installed: %d>%d", args.LeaderId, rf.log.snapLastIndex, args.LastIncludedIndex)
		return
	}

	// install the snapshot in the memory/persister/app todo: 每次回退时碰到snapshot截断，都要全量的从头开始同步日志。这样会不会开销太大了?
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLock()
	rf.snapPending = true
	rf.applyCond.Signal()
}

// leader发送installSnapshot请求
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installToPeer(peer int, term int64, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, int(rf.currentTerm), DLog, "-> S%d, Lost or crashed", peer)
		return
	}
	LOG(rf.me, int(rf.currentTerm), DDebug, "-> S%d, SendSnapshot, Reply=%v", peer, reply.String())

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// check context lost
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, int(rf.currentTerm), DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
		return
	}

	// update match and next index
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}
