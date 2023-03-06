package raft

import "fmt"

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
	fmt.Println("this method has been called")
	if rf.myStatus != Leader{
		println("")
	}

	// step 1, check if is necessary to make the snapshot

	// case 1: index greater than current commitIndex
	if index > rf.commitIndex{
		// cannot make a snapshot for uncommitted data
		return
	}

	// case 2: index smaller or equal than last snapshot index, it means snapshot has already be done
	if index <= rf.lastSnapshotIndex{
		return
	}


	// case 3: make the snapshot

	// step 1: update own information
	previousSnapIndex := rf.lastSnapshotIndex
	fmt.Printf("[%d]: cut from [%d] to [%d] \n", rf.me ,len(rf.logs), len(rf.logs) - (index - rf.lastSnapshotIndex) - 1)
	rf.lastSnapshotTerm = rf.logs[rf.getActuallyIndex(index - rf.lastSnapshotIndex)].Term
	temp := rf.logs[rf.getActuallyIndex(index - rf.lastSnapshotIndex)].Cmd
	println(temp)
	rf.lastSnapshotIndex = index

	// step 2: drop the previous snapshot
	rf.logs = rf.logs[index - previousSnapIndex:] // not need to convert
	rf.logs[0].Cmd = nil
	rf.logs[0].Term = rf.lastSnapshotTerm

	rf.persist()

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)





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
	if !(args.PrevLogIndex >= len(rf.logs) + rf.lastSnapshotIndex) && rf.getActuallyIndex(args.PrevLogIndex) < 0{
		println("qwq")
		// there is the probs, args.PrevLogIndex < args.PrevLogIndex and rf.getActuallyIndex(args.PrevLogIndex) is negative
	}
	if args.PrevLogIndex >= len(rf.logs) + rf.lastSnapshotIndex || args.PrevLogTerm != rf.logs[rf.getActuallyIndex(args.PrevLogIndex)].Term {
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
		rf.logs = rf.logs[:rf.getActuallyIndex(args.PrevLogIndex + 1)]
		rf.logs = append(rf.logs, args.Logs...)
	}
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.getActuallyIndex(rf.lastApplied)].Cmd,
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

	rf.debugLargest = args.PrevLogIndex + 1
	//rf.lastIncludedIndex = args.PrevLogIndex + 1
	return
}


func (rf *Raft) SendSnapshot(server int,args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	// not need to lock or unlock, cause this method will only be called when lock is acquired (? really, or wait for some time?)

	// this method should be called by sendAppendEntries, the following code should be used in sendAppendEntries !!!

	//args := &InstallSnapshotArgs{
	//	Term: rf.currentTerm,
	//	LeaderId: rf.me,
	//	LastIncludedIndex: rf.lastSnapshotIndex,
	//	LastIncludedTerm: rf.lastSnapshotTerm,
	//	Data: rf.persister.ReadSnapshot(),
	//}
	//reply := &InstallSnapshotReply{
	//
	//}

	ok := rf.peers[server].Call("Raft.AcceptSnapshot", args, reply)
	for !ok {
		ok = rf.peers[server].Call("Raft.AcceptSnapshot", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed(){
		return
	}

	if rf.myStatus != Leader{
		return
	}

	if reply.Status == SnapshotLeaderTermOutOfDate{
		rf.myStatus = Follower
		rf.voteFor = -1
		rf.persist()
		return
	}

	if reply.Status == SnapshotSuccess{
		// first, we need to check if this reply is out-of-date

		if rf.nextIndexs[server] >= args.LastIncludedIndex + 1{
			// out-of-date
		}else {
			// not out-of-date
			rf.nextIndexs[server] = args.LastIncludedIndex + 1
		}
	}




}

func (rf *Raft) AcceptSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// case 1: follower killed
	if rf.killed() {
		reply.Term = -1
		reply.Status = SnapshotFollowerKilled
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// case 2: the term of snapshot is lower than the follower current status
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Status = SnapshotLeaderTermOutOfDate
		return
	}

	// update status
	reply.Term = args.Term
	rf.myStatus = Follower
	rf.currentTerm = args.Term
	rf.voteFor = -1
	rf.persist()
	rf.timer.Reset(HeartBeatTimeout)

	// case 3: is already up-to-date
	if args.LastIncludedIndex == rf.lastIncludedIndex{
		reply.Status = SnapshotLogEqualToFollower
		return
	}


	// case 4: success

	// step 1: discard any existing or partial snapshot with a smaller index
	temp := make([]LogEntry, 0)
	if args.LastIncludedIndex > rf.lastIncludedIndex{
		// do nothing, since it's not even complete
		rf.lastIncludedIndex = args.LastIncludedIndex
		temp = append(temp, LogEntry{})
	}else {
		temp = append(temp, rf.logs[args.LastIncludedIndex - rf.lastSnapshotIndex :]...) // not need to convert
	}
	rf.logs = temp

	// step 2: update index
	if args.LastIncludedIndex > rf.commitIndex{
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.logs[0].Term = args.LastIncludedTerm
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.lastSnapshotIndex = args.LastIncludedIndex


	// step 3: save the data
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)


	// step 4: commit the snapshot
	rf.applyChan <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	reply.Status = SnapshotSuccess
}
