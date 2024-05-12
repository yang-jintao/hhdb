package raft

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log.at(i))
		}
		rf.mu.Unlock()

		for i := 0; i < len(entries); i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: entries[i].CommandValid,
				Command:      entries[i].Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}
		}

		rf.mu.Lock()
		LOG(rf.me, int(rf.currentTerm), DApply, "Apply log for [%d, %d]",
			rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}
