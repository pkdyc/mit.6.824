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
	PrevLogTerm  int32
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = args.Term
	reply.VoteGranted = false


	if args.Term < rf.currentTerm{
		reply.VoteGranted  = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		if rf.status == Candidate{
		 	rf.setCandidateToFollower()
		}

		if rf.status == Leader{
		 	rf.setLeaderToFollower()
		}
	}


	//if (rf.votedFor == rf.me || rf.votedFor == -1) && args.Term > rf.currentTerm{
	//	rf.currentTerm = args.Term
	//	rf.votedFor = args.CandidateId
	//	reply.VoteGranted = true
	//	if rf.status == Candidate{
	//		rf.setCandidateToFollower()
	//	}
	//
	//	if rf.status == Leader{
	//		rf.setLeaderToFollower()
	//	}
	//	rf.setVoted()
	//}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[%d] received heartbeat in term [%d] from [%d] with term [%d] \n", rf.me , rf.currentTerm, args.LeaderId, args.Term)

	reply.Term = args.Term

	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		println("drop the fucking dick")
		return
	}

	if args.Term > rf.currentTerm{
		reply.Success = true
		fmt.Printf("damn it bruh, i am [%d]\n",rf.me)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.status == Candidate{
			println("damn it bruh i setCandidateToFollower")
			rf.setCandidateToFollower()
		}else if rf.status == Leader{
			println("damn it bruh i setLeaderToFollower")
			rf.setLeaderToFollower()
		}
	}


	rf.setHeartBeat()
	reply.Term = rf.currentTerm
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
		log.Printf("[%d] received ticket in term [%d] from [%d] \n", rf.me , rf.currentTerm, server)
		rf.cntVoted += 1
		if rf.cntVoted > len(rf.peers) / 2{
			rf.setElectionWin()
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply){
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok{
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader || rf.currentTerm > reply.Term{
		return
	}

	if rf.currentTerm < reply.Term{
		println("wtf!!")
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.setLeaderToFollower()

	}

}


//
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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
					go rf.broadcast()
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
					go rf.broadcast()
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

	if rf.status != Candidate{
		return
	}

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.cntVoted = 1
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}

	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		go rf.sendRequestVote(i, &args , &RequestVoteReply{})
	}


}

func (rf *Raft) broadcast()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader{
		return
	}

	args := AppendEntriesArgs{Term: rf.currentTerm,LeaderId: rf.me}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	}


}

func (rf *Raft) updateStatus (status int32){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == status{
		return
	}
	oldState := rf.status
	switch status {
	case Follower:
		rf.status = Follower
	case Candidate:
		rf.cntVoted = 1
		rf.status = Candidate
	case Leader:
		rf.status= Leader

	default:
		log.Fatalf("unknown status %d\n", status)
	}
	log.Printf("In term [%d], machine [%d] update state from [%d] to [%d]\n",rf.currentTerm, rf.me, oldState, rf.status )
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
		fmt.Printf("[%d] setLeaderToFollower\n",rf.me)
		rf.leaderToFollower <- true
	}()
}


func (rf *Raft) setCandidateToFollower(){
	go func(){
		println("setCandidateToFollower")
		rf.candidateToFollower <- true
	}()
}

