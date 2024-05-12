package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type Role string

const (
	Leader    Role = "leader"
	Follower  Role = "follower"
	Candidate Role = "candidate"

	electionTimeoutMin time.Duration = 200
	electionTimeoutMax time.Duration = 400
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int64
	VoteGranted bool
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//	开始选举
		if rf.role != Leader && rf.electionIsTimeout() {
			rf.becomeCandidate()
			go rf.startElection(rf.currentTerm)

		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) electionIsTimeout() bool {
	if time.Since(rf.electionStart) > rf.electionTimeout {
		return true
	}

	return false
}

func (rf *Raft) resetElectionTimeout() {
	randomTime := electionTimeoutMin + time.Duration(rand.Int63()%int64(electionTimeoutMax-electionTimeoutMin))

	rf.electionStart = time.Now()
	rf.electionTimeout = randomTime
}

func (rf *Raft) becomeFollower(term int64) {
	if term < rf.currentTerm {
		return
	}

	//fmt.Println("becomeFollower")
	LOG(rf.me, int(rf.currentTerm), DLog, "%s -> Follower, For T%d->T%d",
		rf.role, rf.currentTerm, term)

	rf.role = Follower

	shouldPersit := rf.currentTerm != term
	// 	如果当前任期小，说明自己是leader、follower或者是candidate但是版本落后，可能是出现了分区，然后就可以走一步，因为他们肯定不需要参与这一轮的投票，所以VotedFor应当为-1，表示清除投票
	// 如果任期相同，说明只本来是 Candidate ，它一定投了自己。而论文中说的是不能在同一 term 中投两次，因此不能清除投票。即，一旦投了，就不能再撤回甚至改投了。
	if term > rf.currentTerm {
		rf.VotedFor = -1
	}
	rf.currentTerm = term

	//term 是有可能不变的。在 term 不变时，并不需要 persist 因为 term 不变，votedFor 一定不会被重新赋值。
	if shouldPersit {
		rf.persistLock()
	}
}

func (rf *Raft) becomeCandidate() {
	// 领导者不能直接成为候选者，必须先成为跟随者，再变为候选者
	if rf.role == Leader {
		//fmt.Println("becomeCandidate leader")
		LOG(rf.me, int(rf.currentTerm), DError, "Leader can't become Candidate")
		return
	}

	//fmt.Println("becomeCandidate")
	LOG(rf.me, int(rf.currentTerm), DVote, "%s -> Candidate, For T%d->T%d",
		rf.role, rf.currentTerm, rf.currentTerm+1)

	rf.role = Candidate
	rf.currentTerm++
	rf.VotedFor = rf.me

	// 持久化更新的数据
	rf.persistLock()
}

func (rf *Raft) becomeLeader() {
	// 只有候选人才有资格变为leader
	if rf.role != Candidate {
		return
	}

	//fmt.Println("becomeLeader")
	LOG(rf.me, int(rf.currentTerm), DLeader, "%s -> Leader, For T%d",
		rf.role, rf.currentTerm)

	rf.role = Leader

	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int64) {
	var voted int64
	requestVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)
		if !ok {
			//fmt.Println("startElection")
			LOG(rf.me, int(rf.currentTerm), DDebug, "Ask vote from %d, Lost or error", peer)
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.contextLostLocked(Candidate, term) {
			//fmt.Println("startElection")
			LOG(rf.me, int(rf.currentTerm), DVote, "Lost context, abort RequestVoteReply in T%d", rf.currentTerm)
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		if reply.VoteGranted {
			//voted++ // 会有竞争的问题吗
			atomic.AddInt64(&voted, 1)
		}

		if voted > int64(len(rf.peers)/2) {
			rf.becomeLeader()
			go rf.replicationTicker(term)
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Candidate, term) {
		return
	}

	l := rf.log.size()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			voted++
			continue
		}

		requestArgs := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log.at(l - 1).Term,
			//CandidateId: rf.VotedFor,
		}

		go requestVoteFromPeer(peer, requestArgs)

	}
}
