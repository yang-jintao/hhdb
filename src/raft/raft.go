package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"hhdb/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	CurrentTerm int64
	VotedFor    int // 投票给谁成为leader，那个被投票人的ID

	electionStart   time.Time
	electionTimeout time.Duration

	log        []LogEntry
	nextIndex  []int
	matchIndex []int

	commitIndex int           // 全局日志提交进度
	lastApplied int           // 本 Peer 日志 apply 进度
	applyCond   *sync.Cond    // 唤醒 apply 的工作流
	applyCh     chan ApplyMsg // apply 的过程，就是将 applyMsg 通过构造 Peer 时传进来的 channel 返回给应用层。因此还需要保存下这个 applyCh
}

const (
	InvalidTerm  int64 = 0
	InvalidIndex int   = 0
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int64, bool) {
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

//// restore previously persisted state.
//func (rf *Raft) readPersist(data []byte) {
//	if data == nil || len(data) < 1 { // bootstrap without any state?
//		return
//	}
//	// Your code here (PartC).
//	// Example:
//	// r := bytes.NewBuffer(data)
//	// d := labgob.NewDecoder(r)
//	// var xxx
//	// var yyy
//	// if d.Decode(&xxx) != nil ||
//	//    d.Decode(&yyy) != nil {
//	//   error...
//	// } else {
//	//   rf.xxx = xxx
//	//   rf.yyy = yyy
//	// }
//}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果不是leader，则返回错误，因为只有leader有资格写数据
	if rf.role != Leader {
		return 0, 0, false
	}

	// 添加日志请求
	rf.log = append(rf.log, LogEntry{
		Term:         rf.CurrentTerm,
		CommandValid: true,
		Command:      command,
	})
	LOG(rf.me, int(rf.CurrentTerm), DLeader, "Leader accept log [%d]T%d", len(rf.log)-1, rf.CurrentTerm)

	// 持久化日志
	rf.persistLock()

	return len(rf.log) - 1, int(rf.CurrentTerm), true
}

func (rf *Raft) logString() string {
	var terms string
	prevTerm := rf.log[0].Term
	prevStart := 0
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, i-1, prevTerm)
			prevTerm = rf.log[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, len(rf.log)-1, prevTerm)
	return terms
}

func (rf *Raft) firstLogFor(term int64) int {
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) contextLostLocked(role Role, term int64) bool {
	return !(rf.CurrentTerm == term && rf.role == role)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1

	rf.log = append(rf.log, LogEntry{})
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (PartA, PartB, PartC).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applyTicker()

	return rf
}
