package raft

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//
// example RequestVote RPC handler.
//
// 投票过程
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.VoteErr = RaftKilled
		return
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm { // 请求term更小，不投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.VoteErr = VoteReqOutofDate
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.myStatus = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	// 选举限制
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = false
		reply.VoteErr = CandidateLogTooOld
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term &&
		args.LastLogIndex < len(rf.logs)-1 {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = false
		reply.VoteErr = CandidateLogTooOld
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm {
		reply.Term = args.Term
		// 已经投过票,且投给了同一人,由于某些原因，之前的resp丢失
		if rf.voteFor == args.Candidate {
			rf.myStatus = Follower
			rf.timer.Reset(rf.voteTimeout)
			reply.VoteGranted = true
			reply.VoteErr = VotedThisTerm
			rf.mu.Unlock()
			return
		}
		// 来自同一Term不同Candidate的请求，忽略
		if rf.voteFor != -1 {
			reply.VoteGranted = false
			reply.VoteErr = VotedThisTerm
			rf.mu.Unlock()
			return
		}
	}

	// 可以投票
	rf.currentTerm = args.Term
	rf.voteFor = args.Candidate
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	reply.VoteErr = Nil
	rf.persist()
	rf.mu.Unlock()
	return
}

// AppendEntries 心跳包/log追加
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.AppendErr = AppendErr_RaftKilled
		reply.Success = false
		return
	}
	rf.mu.Lock()
	// 无效消息
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_ReqOutofDate
		reply.NotMatchIndex = -1
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)

	// 不匹配
	if args.PrevLogIndex >= len(rf.logs) || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_LogsNotMatch
		reply.NotMatchIndex = rf.lastApplied + 1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.lastApplied > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_Commited
		reply.NotMatchIndex = rf.lastApplied + 1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// 处理日志
	if args.Logs != nil {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Logs...)
	}
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied].Cmd,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.AppendErr = AppendErr_Nil
	reply.NotMatchIndex = -1
	rf.persist()
	rf.mu.Unlock()
	return
}


//func (rf *Raft) AcceptSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
//	// case 1: follower killed
//	if rf.killed() {
//		reply.Term = -1
//		reply.Status = SnapshotFollowerKilled
//		return
//	}
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	// case 2: the term of snapshot is lower than the follower current status
//	if args.Term < rf.currentTerm{
//		reply.Term = rf.currentTerm
//		reply.Status = SnapshotLeaderTermOutOfDate
//		return
//	}
//
//	// update status
//	reply.Term = args.Term
//	rf.myStatus = Follower
//	rf.currentTerm = args.Term
//	rf.voteFor = -1
//	rf.persist()
//
//
//	// case 3: is smaller than follower
//	if args.LastIncludedIndex < rf.lastIncludedIndex{
//		reply.Status = SnapshotLogLowerThanFollower
//		return
//	}
//
//
//	// case 4: is already up-to-date
//	if args.LastIncludedIndex == rf.lastIncludedIndex{
//		reply.Status = SnapshotLogEqualToFollower
//		return
//	}
//
//
//	// case 5: success
//
//	// step 1: discard any existing or partial snapshot with a smaller index
//	temp := make([]LogEntry, 1)
//	temp[0] = LogEntry{}
//	temp = append(temp, rf.logs[args.LastIncludedIndex:]...)
//	rf.logs = temp
//
//	// step 2: update index
//	if args.LastIncludedIndex > rf.commitIndex{
//		rf.commitIndex = args.LastIncludedIndex
//		rf.lastApplied = args.LastIncludedIndex
//	}
//
//
//	rf.persister.SaveStateAndSnapshot(rf.persist(), args.Data)
//
//	/***********************************/
//
//	//接收发来的快照，并提交一个命令处理
//	rf.applyCh <- ApplyMsg{
//		SnapshotValid: true,
//		Snapshot:      args.Data,
//		SnapshotTerm:  args.LastIncludedTerm,
//		SnapshotIndex: args.LastIncludedIndex,
//	}
//
//
//
//
//
//
//
//
//
//
//}
