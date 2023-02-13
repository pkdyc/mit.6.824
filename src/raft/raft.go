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
	"log"
	"math"
	"math/rand"
	// "bytes"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)


//
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
type LogEntry struct {
	Command interface{}
	Term    int
	Index 	 int
}


const (
	Follower  int32 = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int32
	votedFor  int
	log   []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	// customized
	status int32
	timer  *time.Timer
	cntVoted int


	heartBeat chan bool
	winElect chan bool
	leaderToFollower chan bool
	candidateToFollower chan bool
	voted     chan bool
	applyCh   chan ApplyMsg



}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.currentTerm
	var isleader = rf.status == Leader
	// Your code here (2A).

	return int(term), isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term   int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term   int32
	VoteGranted  bool
}

type AppendEntriesArgs struct {
	Term    int32
	LeaderId   int
	PrevLogIndex int
	PrevLogTerm  int
	Entries   []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int32
	Success   bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) requestVoteGranted(args *RequestVoteArgs, reply *RequestVoteReply){
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	if rf.status == Candidate{
		rf.setCandidateToFollower()
	}

	if rf.status == Leader{
		rf.setLeaderToFollower()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = args.Term
	reply.VoteGranted = false

	fmt.Printf("Received vote from [%d] with [%#v] \n",args.CandidateId, args)


	if args.Term < rf.currentTerm{
		reply.VoteGranted  = false
		reply.Term = rf.currentTerm
		return
	}

	// greater term doesn't mean it will be the leader
	// it does help others to update the currentTerm
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		// winning case 0: greater term and the current log is empty
		if len(rf.log) == 0 {
			println("winning case 0")
			rf.requestVoteGranted(args, reply)
		}
	}

	// what needs to be checked at first are the voteFor, for the same term,it might be possible to vote for
	// multiple time,therefore we need to set the constraint at first
	if  (rf.votedFor == rf.me || rf.votedFor == -1) && args.Term == rf.currentTerm && len(rf.log) > 0{
		// winning case two : with newer log


		entry := rf.log[len(rf.log) - 1]
		// case 1 : different term:
		if int(args.LastLogTerm) > entry.Term{
			println("winning case 1")
			rf.requestVoteGranted(args, reply)
		}
		// case 2: same term, but with greater log index
		if int(args.LastLogTerm) == entry.Term && args.LastLogIndex >= entry.Index{
			println("winning case 2")
			rf.requestVoteGranted(args, reply)
		}
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply){
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok{
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Candidate || rf.currentTerm > reply.Term{
		return
	}

	if reply.Term > rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setCandidateToFollower()
		return
	}

	if reply.VoteGranted{
		//log.Printf("[%d] received ticket in term [%d] from [%d] \n", rf.me , rf.currentTerm, server)
		rf.cntVoted += 1
		if rf.cntVoted > len(rf.peers) / 2{
			rf.setElectionWin()
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, successCnt *int){
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok{
		// error in sending, normally not gonna happen
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// case 0: is not leader anymore
	if rf.status != Leader{
		return
	}

	if rf.currentTerm < reply.Term{
		println("wtf!!")
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setLeaderToFollower()

	}


	// case 1: received reply.Success==false as reply, decrease the value of nextIndex,until it reach 1 (? does it will go down 1?)
	if reply.Success == false{
		rf.nextIndex[server] = rf.nextIndex[server] - 1
		if rf.nextIndex[server] < 0{
			//log.Fatalln("the value of nextIndex[server] goes under than 0")
		}
		return
	}
	if reply.Success == true && reply.Term == rf.currentTerm{
		*successCnt += 1
		rf.matchIndex[server] = len(rf.log)
		if len(rf.log) > 0{
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}
	//fmt.Printf("Success Current [%#v] in Term [%#v] \n", *successCnt, rf.currentTerm)


	if *successCnt > len(rf.peers) / 2{
		if len(rf.log) == 0 || rf.log[len(rf.log) - 1].Term != int(rf.currentTerm) {
			return
		}

		for rf.lastApplied < len(rf.log) {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg
			rf.commitIndex = rf.lastApplied
		}
		*successCnt = 0
	}

	return





}

// Start
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
		return len(rf.log), int(rf.currentTerm), false
	}else {
		// case 2: is the leader
		entry := LogEntry{
			Command: command,
			Term:    int(rf.currentTerm),
			Index:   len(rf.log) + 1,
		}
		rf.log = append(rf.log, entry)
		var index = len(rf.log)
		var term = int(rf.currentTerm)
		var isLeader = rf.status == Leader  // must be true
		fmt.Printf("[%#v] [%#v] [%#v] leader id [%#v] with value [%#v]\n", index, term, isLeader, rf.me, command)
		return index, term, isLeader
	}

	// Your code here (2B).


}

//
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		for  {
			rf.mu.Lock()
			state := rf.status
			rf.mu.Unlock()
			switch state {
			case Follower:
				select {
				case <- rf.voted:
				case <- rf.heartBeat:
				case <- time.After(randTime()):
					rf.updateStatus(Candidate)
					go rf.startElection()
				}
			case Candidate:
				select {
				case <- rf.winElect:
					rf.updateStatus(Leader)
					go rf.broadcastAppendEntries()
				case <- rf.candidateToFollower:
					rf.updateStatus(Follower)
				case <- rf.heartBeat:
					rf.updateStatus(Follower)
				case <- time.After(randTime()):
					go rf.startElection()
				}
			case Leader:
				select {
				case <- rf.leaderToFollower:
					rf.updateStatus(Follower)
				case <- time.After(60 * time.Millisecond):
					go rf.broadcastAppendEntries()
				}


			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartBeat = make(chan bool)
	rf.winElect = make(chan bool)
	rf.leaderToFollower = make(chan bool)
	rf.candidateToFollower = make(chan bool)
	rf.voted = make(chan bool)
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.status = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.timer = time.NewTimer(randTime())

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func randTime() time.Duration  {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration((r.Intn(150)) + 150)
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
	rf.cntVoted = 1

	var args RequestVoteArgs
	if len(rf.log) > 0{
		entry := rf.log[len(rf.log) - 1]
		args = RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: entry.Index,
			LastLogTerm: int32(entry.Term),
		}
	}else {
		args = RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	}


	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		go rf.sendRequestVote(i, &args , &RequestVoteReply{})
	}


}

//func (rf *Raft) broadcastHeartBeat()  {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	if rf.status != Leader{
//		return
//	}
//
//	args := AppendEntriesArgs{Term: rf.currentTerm,LeaderId: rf.me}
//
//	for i := range rf.peers {
//		if i == rf.me {
//			continue
//		}
//		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
//	}
//
//}

func (rf *Raft) broadcastAppendEntries()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// case 0: not the leader
	if rf.status != Leader{
		return
	}
	//fmt.Println("heart beating!!!")

	var successCnt = 1 // including leader itself
	// case 1: the log entry is empty, (same behavior to the heartbeat)
	if len(rf.log) == 0{
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

		//temp := int(math.Max(float64(rf.nextIndex[i]-1), 1))

		preLogIdx := rf.nextIndex[i] - 1
		//if len(rf.log) > 0{
		//	preLogIdx = int(math.Min(float64(preLogIdx), float64(len(rf.log)-1)))
		//	println("this is my bad")
		//}
		if preLogIdx > len(rf.log) - 1{
			preLogIdx = len(rf.log)
			idx := len(rf.log) - 1

			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogTerm: rf.log[idx].Term,
				PrevLogIndex: idx,
				Entries: nil,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{}, &successCnt)
			continue
		}

		entry := rf.log[preLogIdx]
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogTerm: entry.Term,
			PrevLogIndex: preLogIdx,
			Entries: rf.log[preLogIdx:],
			LeaderCommit: rf.commitIndex,
		}

		if len(rf.log) == 1{
			println("")
		}

		if len(rf.log) == 2{
			println("")
		}

		fmt.Printf("[%#v] : Sending [%#v] to server [%#v] \n", rf.me, args.Entries, i)

		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{}, &successCnt)
	}

}

func (rf *Raft) commitLog(args *AppendEntriesArgs){
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("[%d] received heartbeat in term [%d] from [%d] with term [%d] \n", rf.me , rf.currentTerm, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	//var isEmpty = len(rf.log) == 0

	fmt.Printf("Append entry from [%#v] with data [%#v] \n", args.LeaderId , args)
	// case 0: if args.term < rf.currentTerm return false directly
	if args.Term < rf.currentTerm{
		// case 0: expired leader message
		println("refuse append entries case 0")
		println("drop the incorrect leader message")
		return
	}



	// case 1: follow the leader
	if args.Term > rf.currentTerm {
		// case 1: receive message from leader with greater term
		reply.Success = true
		fmt.Printf("damn it bruh, i am [%d]\n", rf.me)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.status == Candidate {
			rf.setCandidateToFollower()
		} else if rf.status == Leader {
			rf.setLeaderToFollower()
		}
	}


	rf.setHeartBeat()


	if len(args.Entries) == 0 {
		// just a normal heartbeat, do nothing
		reply.Success = true
		rf.commitLog(args)
		return
	}

	// special case: the node is totally empty
	if len(rf.log) == 0{
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true

		rf.commitLog(args)
		return
	}

	// case 3: append entries (if log doesn't contain and entry at prevLogIndex)

	// conflict 1: pre-log_index > rf.current_log
	// truncate the rest of it, the same as the one in case 4
	if len(rf.log) < args.PrevLogIndex{
		println("refuse append entries case 1")
		return
	}

	// case 4: if an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follow it

	// conflict 2: unmatched log at the same index
	if rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm{
		println("refuse append entries case 2")
		rf.log = rf.log[:args.PrevLogIndex - 1]
		return
	}


	// case 5: append any new entries not already in log, when the code move into this section
	// it does guarantee that  rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
	reply.Success = true
	rf.log = append(rf.log, args.Entries...)


	rf.commitLog(args)






	//







}

func (rf *Raft) updateStatus (status int32){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == status{
		return
	}
	//oldState := rf.status
	switch status {
	case Follower:
		rf.status = Follower
	case Candidate:
		rf.cntVoted = 1
		rf.status = Candidate
		fmt.Printf("[%#v] i start to do the election with term [%#v] and log [%#v] with detail [%#v] \n", rf.me, rf.currentTerm, len(rf.log), rf.log)
	case Leader:
		rf.status= Leader
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log) + 1
			fmt.Printf("set the value of nextIndex[%#v] to be [%#v] \n", i , rf.nextIndex[i])
		}

	default:
		log.Fatalf("unknown status %d\n", status)
	}
	//log.Printf("In term [%d], machine [%d] update state from [%d] to [%d]\n",rf.currentTerm, rf.me, oldState, rf.status )
}

func (rf *Raft) setHeartBeat(){
	go func(){
		rf.heartBeat <- true
	}()
}


func (rf *Raft) setElectionWin(){
	go func(){
		rf.winElect <- true
	}()
}


func (rf *Raft) setLeaderToFollower(){
	go func(){
		//fmt.Printf("[%d] setLeaderToFollower\n",rf.me)
		rf.leaderToFollower <- true
	}()
}


func (rf *Raft) setCandidateToFollower(){
	go func(){
		println("setCandidateToFollower")
		rf.candidateToFollower <- true
	}()
}

