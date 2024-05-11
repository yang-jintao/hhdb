package raft

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	//reply.VoteGranted = false 写到下面12行了
	if rf.CurrentTerm > args.Term {
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

	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, int(rf.CurrentTerm), DVote, "-> S%d, Reject Vote, S%d's log less up-to-date", args.CandidateId)
		return
	}

	// 在这里设置新的投票结果
	reply.VoteGranted = true
	rf.VotedFor = args.CandidateId
	rf.resetElectionTimeout()
	LOG(rf.me, int(rf.CurrentTerm), DVote, "-> S%d, Vote granted", args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// For debug
	LOG(rf.me, int(rf.CurrentTerm), DDebug, "<- S%d, Receive log, Prev=[%d]T%d, Len()=%d",
		args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		LOG(rf.me, int(rf.CurrentTerm), DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}

	if args.Term >= rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	}
	// 选举重制
	rf.resetElectionTimeout()

	// 如果leader的最近的log索引比当前节点的日志大小还要大，说明日志肯定没匹配上，退出
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, int(rf.CurrentTerm), DLog2,
			"<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	// 相同索引的term不同，退出
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		LOG(rf.me, int(rf.CurrentTerm), DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d",
			args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 将leader日志复制到当前节点
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	LOG(rf.me, int(rf.CurrentTerm), DLog2, "Follower append logs: (%d, %d]",
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 如果leader的commitIndex大于follower的commitIndex，那么follower就用apply日志
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, int(rf.CurrentTerm), DApply, "Follower update the commit index %d->%d",
			rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		// 不理解
		if rf.commitIndex >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		}
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) isMoreUpToDateLocked(candidateIndex int, candidateTerm int64) bool {
	l := len(rf.log)
	lastTerm, lastIndex := rf.log[l-1].Term, l-1
	LOG(rf.me, int(rf.CurrentTerm), DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)

	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}

	return lastIndex > candidateIndex
}
