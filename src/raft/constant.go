package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

var HeartBeatTimeout = 120 * time.Millisecond

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



// Raft
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
	currentTerm int
	votedFor int
	logs     []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	// customized
	status int32


	heartBeat chan bool
	winElect chan bool
	leaderToFollower chan bool
	candidateToFollower chan bool
	voted     chan bool
	applyCh   chan ApplyMsg


	timer       *time.Ticker

	voteTimeout time.Duration



}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term   int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term   int
	VoteGranted  bool
	State 		VoteErr
}


type VoteErr int64

const (
	VoteSuccess           VoteErr = iota //投票过程无错误
	VoteReqHasSmallerTerm                //投票消息过期
	CandidateLogTooOld                   //候选人Log不够新
	AlreadyVotedThisTerm                 //本Term内已经投过票
	Killed                               //Raft程已终止
)


type AppendEntriesArgs struct {
	Term    int
	LeaderId   int
	PrevLogIndex int
	PrevLogTerm  int
	Entries   []LogEntry
	LeaderCommit int
	LeaderLength int
}

type AppendEntriesReply struct {
	Term    int
	Success   bool
	State 	AppendEntriesErr
	MatchIndex int
}


type AppendEntriesErr int64

const (
	AppendErr_Nil          AppendEntriesErr = iota // Append操作无错误
	AppendErr_LogsNotMatch                         // Append操作log不匹配
	AppendErr_ReqOutofDate                         // Append操作请求过期
	AppendErr_ReqRepeat                            // Append请求重复
	AppendErr_Commited                             // Append的log已经commit
	AppendErr_RaftKilled                           // Raft程序终止
)

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
	rf.timer = time.NewTicker(rf.voteTimeout)
	rf.logs = []LogEntry{{nil, 0 , 0}}


	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}


func (rf *Raft) ticker() {
	select {
	case <-rf.timer.C:
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		currStatus := rf.status
		switch currStatus {
		case Follower:
			rf.status = Candidate
			fallthrough
		case Candidate:
			rf.startElection()
		case Leader:
			// 进行心跳
			rf.broadcastAppendEntries()
		}
		rf.mu.Unlock()
	}
}
