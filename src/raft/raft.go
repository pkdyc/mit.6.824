package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	// "bytes"
	"sync/atomic"
	"time"

	// "6.824/labgob"
)


//
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.currentTerm
	var isleader = rf.status == Leader
	// Your code here (2A).
	rf.persist()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatal("rf write persist err!")
		return
	}
	err = e.Encode(rf.logs)
	if err != nil {
		log.Fatal("rf write persist err!")
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		log.Fatal("rf write persist err!")
		return
	}
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	err := d.Decode(&rf.currentTerm)
	if err != nil {
		log.Fatal("rf read persist err!")
		return
	}
	err = d.Decode(&rf.logs)
	if err != nil {
		log.Fatal("rf read persist err!")
		return
	}
	err = d.Decode(&rf.votedFor)
	if err != nil {
		log.Fatal("rf read persist err!")
		return
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logs through (and including)
// that index. Raft should now trim its logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}




//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()


	reply.Term = args.Term
	reply.VoteGranted = false

	if rf.killed(){
		// case 1: has been killed
		reply.VoteGranted = false
		reply.Term = -1
		reply.State = Killed
		return
	}



	fmt.Printf("[%#v]: Received vote from [%d] with [%#v] \n",rf.me, args.CandidateId, args)


	if args.Term < rf.currentTerm{
		// case 2: candidate has smaller term
		reply.VoteGranted  = false
		reply.Term = rf.currentTerm
		reply.State = VoteReqHasSmallerTerm
		return
	}

	if args.Term > rf.currentTerm{
		// since acknowledge the greater term, update own status and become follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	entry := rf.logs[len(rf.logs) - 1]
	if (entry.Index > args.LastLogIndex && entry.Term == args.LastLogTerm) || entry.Term > args.LastLogTerm{
		// case 3: the log is too old
		reply.VoteGranted = false
		reply.State = CandidateLogTooOld
		return
	}

	// do I really need to consider it?
	if entry.Index == args.LastLogIndex && entry.Term == args.LastLogTerm && rf.votedFor == args.CandidateId{
		// case : special, received duplicate request from the same server
		reply.VoteGranted = true
		reply.State = VoteSuccess
		rf.status = Follower
		rf.timer.Reset(rf.voteTimeout)
		return
	}

	if !(rf.votedFor == rf.me || rf.votedFor == -1){
		// case 4: already vote
		reply.VoteGranted = false
		reply.State = AlreadyVotedThisTerm
		return
	}


	// case 5: success
	rf.status = Follower
	rf.votedFor = args.CandidateId
	rf.timer.Reset(rf.voteTimeout)
	reply.State = VoteSuccess


}


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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteCount *int){
	if rf.killed() {
		return
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		fmt.Printf("[%#v] : 🤔🤔🤔 sending to [%#v] failed with args [%#v] , damn my logs is [%#v] \n",rf.me, server, args, rf.logs)

		if rf.killed() {
			return
		}
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Candidate || rf.currentTerm > reply.Term{
		// at this time, everything is out of date
		return
	}

	switch reply.State {
	case VoteSuccess:
		*voteCount += 1
		if *voteCount > len(rf.peers) / 2{
			*voteCount = 0
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}
			rf.timer.Reset(HeartBeatTimeout)
		}
	case VoteReqHasSmallerTerm:
		rf.currentTerm = reply.Term
		rf.timer.Reset(rf.voteTimeout)
		rf.status = Follower
		rf.votedFor = -1
	case AlreadyVotedThisTerm:
		// do nothing
	case CandidateLogTooOld:
	case Killed:
	}


}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, successCnt *int){
	if rf.killed() {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		// 失败重传
		fmt.Printf("[%#v] : 🤔🤔🤔 sending to [%#v] failed with args [%#v] , damn my logs is [%#v] \n",rf.me, server, args, rf.logs)

		if rf.killed() {
			return
		}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// case 0: is not leader anymore
	if rf.status != Leader{
		return
	}

	switch rf.status {

	}

	if rf.currentTerm < reply.Term{
		fmt.Printf("[%#v] : wtf i dropper from leader to follower, received the reply from [%#v] with term [%#v], my term is [%#v]\n , damn my logs is [%#v] \n",rf.me, server, reply.Term, rf.currentTerm , rf.logs)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setLeaderToFollower()
		rf.persist()

	}


	// case 1: received reply.VoteSuccess==false as reply, decrease the value of nextIndex,until it reach 1 (? does it will go down 1?)
	if reply.Success == false{
		rf.nextIndex[server] = rf.nextIndex[server] - 1
		fmt.Printf("[%#v] 🔪🔪🔪 sending append entries to [%#v] fail with arg [%#v] \n", rf.me, server, args)
		if rf.nextIndex[server] < 1{
			rf.nextIndex[server] = 1
			//logs.Fatalln("the value of nextIndex[server] goes under than 0")
		}
		return
	}
	if reply.Success == true && reply.Term == rf.currentTerm{
		*successCnt += 1
		temp := rf.nextIndex[server]
		rf.matchIndex[server] = len(rf.logs)

		if args.LeaderLength > 0{
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			fmt.Printf("[%#v] :  🌰🌰🌰 received success from [%#v], with args [%#v] update matchIndex from [%#v] to [%#v]\n", rf.me, server,args, temp,rf.nextIndex[server] )
		}else{
			fmt.Printf("[%#v] :  🌰🌰🌰 received heartbeat success from [%#v], with args [%#v] dont update matchIndex from [%#v] to [%#v]\n", rf.me, server,args, temp,rf.nextIndex[server] )

		}
	}
	//fmt.Printf("VoteSuccess Current [%#v] in Term [%#v] \n", *successCnt, rf.currentTerm)


	if *successCnt > len(rf.peers) / 2{
		fmt.Printf("🐴🐴 able to commit !!! current logs len [%d], lastApplied [%#v] \n", len(rf.logs), rf.lastApplied)
		*successCnt = 0

		if args.LeaderLength == 0 || rf.logs[args.LeaderLength - 1].Term != int(rf.currentTerm) {
			println("💔💔💔 wtf??? commit failed")
			return
		}

		for rf.lastApplied < args.LeaderLength {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg
			rf.commitIndex = rf.lastApplied
			fmt.Printf("😄😄😄 commit [%#v] successfully \n",rf.logs[rf.lastApplied-1])
		}
	}

	return





}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// case 0, has been killed
	if rf.killed(){
		return -1, -1, false
	}

	// case 1: is not the leader
	if rf.status != Leader{
		return len(rf.logs), int(rf.currentTerm), false
	}else {
		// case 2: is the leader
		entry := LogEntry{
			Command: command,
			Term:    int(rf.currentTerm),
			Index:   len(rf.logs) + 1,
		}
		rf.logs = append(rf.logs, entry)
		var index = len(rf.logs)
		var term = int(rf.currentTerm)
		var isLeader = rf.status == Leader  // must be true
		fmt.Printf("[%#v] [%#v] [%#v] leader id [%#v] with value [%#v]\n", index, term, isLeader, rf.me, command)
		rf.persist()
		return index, term, isLeader
	}


	// Your code here (2B).


}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}





func randTime() time.Duration  {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration((r.Intn(150)) + 150	)
}

func (rf *Raft) startElection()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//println("start election")

	if rf.status != Candidate{
		return
	}

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()

	voteCount := 1
	var args RequestVoteArgs
	if len(rf.logs) > 0{
		entry := rf.logs[len(rf.logs) - 1]
		args = RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: entry.Index,
			LastLogTerm: entry.Term,
		}
	}else {
		args = RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	}


	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		go rf.sendRequestVote(i, &args , &RequestVoteReply{}, &voteCount)
	}


}


func (rf *Raft) broadcastAppendEntries()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// case 0: not the leader
	if rf.status != Leader{
		return
	}
	//fmt.Println("heart beating!!!")

	var successCnt = 1 // including leader itself
	// case 1: the logs entry is empty, (same behavior to the heartbeat)
	if len(rf.logs) == 0{
		args := AppendEntriesArgs{Term: rf.currentTerm,LeaderId: rf.me}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{}, &successCnt)
		}
		return
	}

	// case 2


	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		nextIndex := rf.nextIndex[i]
		preLogIdx := nextIndex - 1 // follower上一个log的index编号， 也就是rf.logs[i:] 中 i需要用到的值

		followerCurrentLatestLogIndex := preLogIdx -1 // 对应follower当前最新的log 在array中的index，如果为-1 说明follower log是空的

		// follower log为空
		if followerCurrentLatestLogIndex == -1{
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogTerm: 0,
				PrevLogIndex: 0,
				LeaderCommit: rf.commitIndex,
				LeaderLength: len(rf.logs),
			}
			args.Entries = append(args.Entries, rf.logs[preLogIdx:]...)
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{}, &successCnt)
			fmt.Printf("[%#v] :case logs empty: Sending to server [%#v] with data: [%#v] \n 😅😅😅 my own logs is [%#v] \n", rf.me,i, args, rf.logs)
			continue

		} else {
			// follower log不为空
			entry := rf.logs[followerCurrentLatestLogIndex]
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogTerm: entry.Term,
				PrevLogIndex: entry.Index,
				LeaderCommit: rf.commitIndex,
				LeaderLength: len(rf.logs),
			}
			args.Entries = append(args.Entries, rf.logs[preLogIdx:]...)
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{}, &successCnt)
			fmt.Printf("[%#v] :case logs not empty: Sending to server [%#v] with data: [%#v] \n 😅😅😅 my own logs is [%#v] \n", rf.me,i, args, rf.logs)
			continue

		}



	}

}

func (rf *Raft) commitLog(args *AppendEntriesArgs){
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs))))
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()


	//logs.Printf("[%d] received heartbeat in term [%d] from [%d] with term [%d] \n", rf.me , rf.currentTerm, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false



	fmt.Printf("[%#v] Append entry from [%#v] with data [%#v] \n",rf.me, args.LeaderId , args)
	// case 0: if args.term < rf.currentTerm return false directly



	if args.Term < rf.currentTerm{
		// case 1 : request out of date, the leader has lower term than the receiver
		reply.State = AppendErr_ReqOutofDate
		reply.Success = false
		reply.MatchIndex = -1
		println("refuse append entries case -1")
		println("drop the incorrect leader message")
		return
	}

	rf.timer.Reset(rf.voteTimeout)
	rf.status = Follower
	rf.votedFor = args.LeaderId
	rf.

	rf.setHeartBeat()




	if rf.status == Leader && args.Term <= rf.currentTerm{
		fmt.Printf("😆😆😆 [%#v] Append entry from [%#v] with data [%#v] but i am the leader with term [%#v] \n",rf.me, args.LeaderId , args, rf.currentTerm)
		return
	}



	rf.setHeartBeat()
	// case 1: follow the leader
	if args.Term > rf.currentTerm {
		// case 1: receive message from leader with greater term
		fmt.Printf("damn it bruh, i am [%d]\n", rf.me)
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.persist()
		if rf.status == Candidate {
			rf.setCandidateToFollower()
		} else if rf.status == Leader {
			rf.setLeaderToFollower()
		}
	}






	// special case: the node is totally empty
	if args.PrevLogIndex == 0{
		if len(rf.logs) == 0{
			rf.logs = append(rf.logs, args.Entries...)
			reply.Success = true

			rf.commitLog(args)
			fmt.Printf("[%#v] : 🎉🎉🎉, current logs [%#v] \n",rf.me, rf.logs)
			rf.persist()
			return
		}else {
			rf.logs = nil
			rf.persist()
			println("!!! fuck yeah, how is that even possible !!!")
			return
		}

	}

	if args.PrevLogIndex > len(rf.logs){
		fmt.Printf("[%#v] : refuse append entries case 0 , current logs [%#v] \n", rf.me, rf.logs)
		return
	}

	// case 3: append entries (if logs doesn't contain and entry at prevLogIndex)

	// conflict 1: pre-log_index > rf.current_log
	// truncate the rest of it, the same as the one in case 4
	if len(rf.logs) < args.PrevLogIndex{
		fmt.Printf("[%#v] : refuse append entries case 1 , current logs [%#v] \n", rf.me, rf.logs)
		return
	}

	// case 4: if an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follow it

	// conflict 2: unmatched logs at the same index


	if rf.logs[args.PrevLogIndex - 1].Term != args.PrevLogTerm{
		fmt.Printf("[%#v] : refuse append entries case 3 , current logs [%#v] \n", rf.me, rf.logs)
		rf.logs = rf.logs[:args.PrevLogIndex - 1]
		return
	}


	if len(rf.logs) > args.PrevLogIndex{
		rf.logs = rf.logs[:args.PrevLogIndex]
	}


	// case 5: append any new entries not already in logs, when the code move into this section
	// it does guarantee that  rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm
	reply.Success = true
	//rf.logs = rf.logs[args.PrevLogIndex - 1:]
	rf.logs = append(rf.logs, args.Entries...)

	fmt.Printf("[%#v] : 🎉🎉🎉, current logs [%#v] \n",rf.me, rf.logs)

	rf.commitLog(args)




	//



}
