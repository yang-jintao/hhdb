package raft

import "fmt"

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	//reply.VoteGranted = false 写到下面12行了
	if rf.CurrentTerm > args.Term {
		fmt.Println("RequestVote")
		LOG(rf.me, int(rf.CurrentTerm), DVote, "-> S%d, Reject vote, higher term, T%d>T%d", args.CandidateId, rf.CurrentTerm, args.Term)
		reply.VoteGranted = false
		return
	}

	if rf.CurrentTerm < args.Term {
		// 在这里面让上一次任期的投票结果给清除
		rf.becomeFollower(args.Term)
		// 不能直接return
		//reply.VoteGranted = true
		//return
	}

	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	// 在这里设置新的投票结果
	reply.VoteGranted = true
	rf.VotedFor = args.CandidateId
	rf.resetElectionTimeout()
	fmt.Println("RequestVote")
	LOG(rf.me, int(rf.CurrentTerm), DVote, "-> S%d, Vote granted", args.CandidateId)
}
