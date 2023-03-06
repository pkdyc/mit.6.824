package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.myStatus = Follower
	rf.voteFor = -1
	rand.Seed(time.Now().UnixNano())
	rf.voteTimeout = time.Duration(rand.Intn(150)+200) * time.Millisecond
	rf.currentTerm, rf.commitIndex, rf.lastApplied = 0, 0, 0
	rf.nextIndexs, rf.matchIndexs, rf.logs = nil, nil, []LogEntry{{0, nil}}
	rf.timer = time.NewTicker(rf.voteTimeout)
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}


func (rf *Raft) broadcastAppendEntries() {
	appendNum := 1
	rf.timer.Reset(HeartBeatTimeout)
	// 构造msg
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		appendEntriesArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Logs:         nil,
			LeaderCommit: rf.commitIndex,
			LogIndex:     len(rf.logs) - 1,
		}
		for rf.nextIndexs[i] > 0 {
			appendEntriesArgs.PrevLogIndex = rf.nextIndexs[i] - 1
			if appendEntriesArgs.PrevLogIndex >= len(rf.logs) {
				rf.nextIndexs[i]--
				continue
			}
			appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex].Term
			break
		}
		if rf.nextIndexs[i] < len(rf.logs) {
			// appendEntriesArgs.Logs = rf.logs[rf.nextIndexs[i]:appendEntriesArgs.LogIndex+1]
			appendEntriesArgs.Logs = make([]LogEntry, appendEntriesArgs.LogIndex+1-rf.nextIndexs[i])
			copy(appendEntriesArgs.Logs, rf.logs[rf.nextIndexs[i]:appendEntriesArgs.LogIndex+1])
		}
		appendEntriesReply := new(AppendEntriesReply)
		go rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply, &appendNum)
	}
}

func (rf *Raft) broadcastRequestVote() {
	// 进行选举
	rf.currentTerm += 1
	rf.voteFor = rf.me
	// 每轮选举开始时，重新设置选举超时
	rf.voteTimeout = time.Duration(rand.Intn(150)+200) * time.Millisecond
	voteNum := 1
	rf.persist()
	rf.timer.Reset(rf.voteTimeout)
	// 构造msg
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		voteArgs := &RequestVoteArgs{
			Term:         rf.currentTerm,
			Candidate:    rf.me,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}
		voteReply := new(RequestVoteReply)
		go rf.sendRequestVote(i, voteArgs, voteReply, &voteNum)
	}
}



func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			currStatus := rf.myStatus
			switch currStatus {
			case Follower:
				rf.myStatus = Candidate
				fallthrough
			case Candidate:
				rf.broadcastRequestVote()
			case Leader:
				// 进行心跳
				rf.broadcastAppendEntries()
			}
			rf.mu.Unlock()
		}
	}
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 客户端的log
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	isLeader = rf.myStatus == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	logEntry := LogEntry{Term: rf.currentTerm, Cmd: command}
	rf.logs = append(rf.logs, logEntry)

	index = len(rf.logs) - 1
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()

	return index, term, isLeader
}

type VoteErr int64

const (
	Nil                VoteErr = iota //投票过程无错误
	VoteReqOutofDate                  //投票消息过期
	CandidateLogTooOld                //候选人Log不够新
	VotedThisTerm                     //本Term内已经投过票
	RaftKilled                        //Raft程已终止
)

type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
	Term         int
	Candidate    int
	LastLogIndex int // 用于选举限制，LogEntry中最后Log的index
	LastLogTerm  int // 用于选举限制，LogEntry中最后log的Term
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your Data here (2A).
	Term        int
	VoteGranted bool    //是否同意投票
	VoteErr     VoteErr //投票操作错误
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int //Leader标识
	PrevLogIndex int //nextIndex前一个index
	PrevLogTerm  int //nextindex前一个index处的term
	Logs         []LogEntry
	LeaderCommit int //Leader已经commit了的Log index
	LogIndex     int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool             // Append操作结果
	AppendErr     AppendEntriesErr // Append操作错误情况
	NotMatchIndex int              // 当前Term的第一个元素（没有被commit的元素）的index
}


func (rf *Raft) persist() {
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistData() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tmpTerm int
	var tmpVoteFor int
	var tmplogs []LogEntry
	var tmplastSnapshotIndex int
	var tmplastSnapshotTerm int
	if d.Decode(&tmpTerm) != nil ||
		d.Decode(&tmpVoteFor) != nil ||
		d.Decode(&tmplogs) != nil ||
		d.Decode(&tmplastSnapshotIndex) != nil||
		d.Decode(&tmplastSnapshotTerm) != nil{
		fmt.Println("decode error")
	} else {
		rf.currentTerm = tmpTerm
		rf.voteFor = tmpVoteFor
		rf.logs = tmplogs
		rf.lastSnapshotIndex = tmplastSnapshotIndex
		rf.lastSnapshotTerm = tmplastSnapshotTerm
	}
}





type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Server当前的term
	voteFor     int // Server在选举阶段的投票目标
	logs        []LogEntry
	nextIndexs  []int // Leader在发送LogEntry时，对应每个其他Server，开始发送的index
	matchIndexs []int
	commitIndex int    // Server已经commit了的Log index
	lastApplied int    // Server已经apply了的log index
	myStatus    Status // Server的状态

	timer       *time.Ticker  // timer
	voteTimeout time.Duration // 选举超时时间，选举超时时间是会变动的，所以定义在Raft结构体中
	applyChan   chan ApplyMsg // 消息channel


	// for snapshot
	lastIncludedIndex int
	lastSnapshotIndex int
	lastSnapshotTerm  int

	debugLargest int
}

type LogEntry struct {
	Term int         // LogEntry中记录有log的Term
	Cmd  interface{} // Log的command
}

var HeartBeatTimeout = 120 * time.Millisecond

type Status int64

const (
	Follower Status = iota
	Candidate
	Leader
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// 获取Server当前的Term和是否是Leader
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.myStatus == Leader
	rf.mu.Unlock()
	return term, isleader
}



type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int //Leader标识
	LastIncludedIndex 	int
	LastIncludedTerm int
	Offset           int
	Data             []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term   int
	Status SnapshotStatus
}

type SnapshotStatus int64

const (
	SnapshotSuccess SnapshotStatus = iota
	SnapshotFollowerKilled
	SnapshotLeaderTermOutOfDate
	SnapshotLogEqualToFollower
)



