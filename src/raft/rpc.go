package raft

//
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
//
// 改造函数，添加了一个参数，用于方便实现同一Term内请求的统计
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm { // 过期请求
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	switch reply.VoteErr {
	case VoteReqOutofDate:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case CandidateLogTooOld:
		// 日志不够新
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case Nil, VotedThisTerm:
		rf.mu.Lock()
		//根据是否同意投票，收集选票数量
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNum <= len(rf.peers)/2 {
			*voteNum++
		}
		if *voteNum > len(rf.peers)/2 {
			*voteNum = 0
			if rf.myStatus == Leader {
				rf.mu.Unlock()
				return ok
			}
			rf.myStatus = Leader
			rf.nextIndexs = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndexs {
				rf.nextIndexs[i] = len(rf.logs)
			}
			rf.timer.Reset(HeartBeatTimeout)
		}
		rf.mu.Unlock()
	case RaftKilled:
		return false
	}
	return ok
}

func (rf *Raft) getActuallyIndex(i int) int{
	return i - rf.lastSnapshotIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm { // 过期消息
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	switch reply.AppendErr {
	case AppendErr_Nil:
		rf.mu.Lock()
		if reply.Success && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
			*appendNum++
		}
		if rf.nextIndexs[server] >= args.LogIndex+1 {
			rf.mu.Unlock()
			return ok
		}
		rf.nextIndexs[server] = args.LogIndex + 1
		if *appendNum > len(rf.peers)/2 {
			*appendNum = 0
			if rf.logs[rf.getActuallyIndex(args.LogIndex)].Term != rf.currentTerm {
				rf.mu.Unlock()
				return false
			}
			for rf.lastApplied < args.LogIndex {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.getActuallyIndex(rf.lastApplied)].Cmd,
					CommandIndex: rf.lastApplied,
				}
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastApplied
			}
		}
		rf.mu.Unlock()
	case AppendErr_ReqOutofDate:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case AppendErr_LogsNotMatch, AppendErr_Commited:
		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndexs[server] = reply.NotMatchIndex
		if reply.NotMatchIndex < rf.lastSnapshotIndex{
			// send the snapshot if the lastSnapshotIndex > NotMatchIndex
			args := &InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: rf.lastSnapshotIndex,
				LastIncludedTerm: rf.lastSnapshotTerm,
				Data: rf.persister.ReadSnapshot(),
			}
			reply := &InstallSnapshotReply{

			}
			go rf.SendSnapshot(server, args, reply)
		}

		rf.mu.Unlock()
	case AppendErr_ReqRepeat:
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.myStatus = Follower
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.timer.Reset(rf.voteTimeout)
			rf.persist()
		}
		rf.mu.Unlock()
	case AppendErr_RaftKilled:
		return false
	}
	return ok
}

