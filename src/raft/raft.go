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
	"bytes"
	"math/rand"
	//"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"fmt"
	"github.com/sasha-s/go-deadlock"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term 		 int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	currentTerm int
	votedFor int
	log []Logentry
	state int
	heartbeat chan bool
	voteGranted chan bool
	voted chan bool
	iskilled chan bool
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	votenum int
	applyCh chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
type Logentry struct{
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 2{
		isleader = true
	}else{
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader       
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Logentry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
	   fmt.Println("error")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = log
	}
	// Your code here (3C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		return
	}

	if args.Term>rf.currentTerm{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.state = 0
		rf.votedFor = -1
		rf.persist() 
	}
	update := false
	LastLogTerm := -1
	LastLogIndex := len(rf.log)
	if LastLogIndex!=0{
		LastLogTerm = rf.log[LastLogIndex-1].Term
	}
	
 
	if args.LastLogTerm>LastLogTerm{
		update = true
	}
	if args.LastLogTerm==LastLogTerm && args.LastLogIndex>=LastLogIndex{
		update = true
	}
	
	if(rf.votedFor==-1 || rf.votedFor==args.CandidateId) && update{
		rf.state = 0
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.voted <- true
		rf.persist()
	}
	return
	// Your code here (3A, 3B).
}

type  AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Logentry
	LeaderCommit int
}
type  AppendEntriesReply struct {
	Term int
	Success bool
	Xterm int
	Xindex int
	Xlen int
	
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	var last_new_index int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term>rf.currentTerm{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		rf.state = 0;
		rf.heartbeat <- true
		rf.persist()
	}
	if args.Term==rf.currentTerm{
		rf.heartbeat <- true
	}
	if args.Term<rf.currentTerm{
		reply.Success = false
		//fmt.Println("term")

		return
	}
	if args.PrevLogIndex>len(rf.log){
		reply.Success = false
		reply.Xterm = -1
		reply.Xlen = len(rf.log)+1

		//fmt.Println("log")

		return
	}
	if args.PrevLogIndex==0{
		reply.Success = true
		index := args.PrevLogIndex
		for _,entry := range args.Entries{
			if(index==len(rf.log)){
				break
			}
			if entry.Term!=rf.log[index].Term{
				rf.log = rf.log[:index]
				break
			}
			index++
		}
		if index==len(rf.log){
			rf.log = append(rf.log[:args.PrevLogIndex],args.Entries...)
			last_new_index = len(rf.log)
		}else{
			last_new_index = index
		}
		if args.LeaderCommit>rf.commitIndex{
			if args.LeaderCommit<last_new_index{
				rf.commitIndex = args.LeaderCommit
			}else{
				rf.commitIndex = last_new_index
			}
		}
		//fmt.Println(rf.me,rf.log)

		return
	}
	var i int
	prevLogTerm := rf.log[args.PrevLogIndex-1].Term
	if prevLogTerm != args.PrevLogTerm{
		reply.Success = false
		reply.Xterm = prevLogTerm
		for i=args.PrevLogIndex-1;i>=0;i--{
			if rf.log[i].Term!= prevLogTerm{
				break
			}
		}
		reply.Xindex = i+1
		//fmt.Println("term1")
		//rf.log = rf.log[:args.PrevLogIndex-1]

	} else{
		index := args.PrevLogIndex
		for _,entry := range args.Entries{
			if(index==len(rf.log)){
				break
			}
			if entry.Term!=rf.log[index].Term{
				rf.log = rf.log[:index]
				break
			}
			index++
		}
		if index==len(rf.log){
			rf.log = append(rf.log[:args.PrevLogIndex],args.Entries...)
			last_new_index = len(rf.log)
		}else{
			last_new_index = index
		}
		reply.Success = true
		if args.LeaderCommit>rf.commitIndex{
			if args.LeaderCommit<last_new_index{
				rf.commitIndex = args.LeaderCommit
			}else{
				rf.commitIndex = last_new_index
			}
		}
		
		//fmt.Println(rf.me,rf.log)
	}
	rf.persist()
	return





	

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return true
	}
	for !rf.peers[server].Call("Raft.RequestVote", args, reply){
		//time.Sleep(10 * time.Millisecond)
		return true
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state!=1{
		return true
	}
	if args.Term!=rf.currentTerm{
		return true
	}
	if reply.Term>rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.state = 0
		rf.votedFor = -1
		rf.persist()
		return true
	}
	if reply.VoteGranted{
		rf.votenum++;
	}

	
	if rf.votenum>(len(rf.peers)/2) && rf.state == 1{
		//fmt.Println(rf.me)
		rf.state=2
		rf.voteGranted <- true
		for i:=0; i<len(rf.peers); i++{
			rf.nextIndex[i] = len(rf.log)+1
			rf.matchIndex[i] = 0
			//fmt.Println("init",len(rf.log)+1)
		}
		//fmt.Println("init",len(rf.log)+1)
		fmt.Println("leader:",rf.me,rf.currentTerm)
		go rf.sendAppendEntries2a()
	//fmt.Println(len(rf.log))
	}
	return true
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return true
	}
	for !rf.peers[server].Call("Raft.AppendEntries", args, reply){
		//time.Sleep(10 * time.Millisecond)
		return true
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term>rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.state = 0
		rf.votedFor = -1
		rf.persist()
		return true
	}
	if rf.state!=2{
		return true
	}
	if args.Term!=rf.currentTerm{
		return true
	}
	var i int
	if reply.Success==false{
		if rf.nextIndex[server] == args.PrevLogIndex+1{
			if reply.Xterm==-1{
				rf.nextIndex[server] = reply.Xlen
			}else{
				for i=args.PrevLogIndex-1;i>=reply.Xindex;i--{
					if rf.log[i].Term==reply.Xterm{
						break
					}
				}
				rf.nextIndex[server] = i+2
			}
			//fmt.Println("false",rf.me,rf.nextIndex[server],len(rf.log))
			//fmt.Println("update",rf.me,rf.nextIndex[server],len(rf.log))
			//rf.nextIndex[server]--
			//fmt.Println(rf.nextIndex[server])
		}
	}
	if reply.Success{
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newNext > rf.nextIndex[server] {
			rf.nextIndex[server] = newNext
			//fmt.Println("true",rf.me,rf.nextIndex[server],len(rf.log))
		}
		if newMatch > rf.matchIndex[i] {
			rf.matchIndex[server] = newMatch
		}
	}
	return true
}


func (rf *Raft) applyfunc()  {
	for !rf.killed(){
		rf.mu.Lock()
		//fmt.Println("apply",rf.me,rf.lastApplied,rf.commitIndex,len(rf.log))
		for i:=rf.lastApplied;i<rf.commitIndex;i++{
			am := ApplyMsg{
				CommandValid :true,
				Command : rf.log[i].Command,
				CommandIndex : i+1,
				Term : rf.currentTerm,
			}
			//fmt.Println("apply",rf.me,am)
			//fmt.Println("a")
			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
			rf.lastApplied++
			//fmt.Println(rf.me,i+1,rf.log[i].Command,rf.state)
			//fmt.Println(rf.log)
			//fmt.Println(rf.me,rf.log[i].Command,rf.state)
		}
		
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//fmt.Println("p")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("q")
	if rf.killed() == true {
		return -1, -1, false
	}
	index := len(rf.log)+1
	term := rf.currentTerm
	isLeader := false
	if rf.state == 2{
		isLeader = true
		log := Logentry{
			Term: rf.currentTerm,
			Command :  command,
		}
		rf.log = append(rf.log,log)
		rf.persist()
		//fmt.Println(rf.me,log.Command)
		//fmt.Println(command)
		
		/*for i:=0; i<len(rf.peers); i++{
			if i==rf.me{
				continue
			}
			args := AppendEntriesArgs{}
			rf.mu.Lock()
			args.Term = rf.currentTerm 
			args.PrevLogIndex =  rf.nextIndex[i]-1
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
			args.Entries = rf.log[args.PrevLogIndex:]
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}*/
	}else{
		return -1, -1, false
	}
	
	// Your code here (3B).


	return index, term, isLeader
}
func (rf *Raft) sendAppendEntries2a() {
	rf.mu.Lock()
	for i:=0; i<len(rf.peers); i++{
		if i==rf.me{
			continue
		}
		go rf.sendAppendEntries2one(i)
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendAppendEntries2one(i int) {
	for !rf.killed(){
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if(rf.state!=2){
			rf.mu.Unlock()
			return
		}
		if len(rf.log)>=rf.nextIndex[i]{
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm 
			args.PrevLogIndex =  rf.nextIndex[i]-1
			if args.PrevLogIndex ==0{
				args.PrevLogTerm = -1
			}else{
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}
			args.Entries = rf.log[args.PrevLogIndex:]
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			
			rf.sendAppendEntries(i, &args, &reply)
			rf.mu.Lock()
			if rf.state!=2{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if rf.killed(){
				return
			}
			success:=reply.Success
			//fmt.Println(success,args.PrevLogIndex)
			for success==false{
				//fmt.Println(i,rf.nextIndex[i])
				//rf.nextIndex[i]--
				args := AppendEntriesArgs{}
				rf.mu.Lock()
				args.Term = rf.currentTerm 
				args.PrevLogIndex =  rf.nextIndex[i]-1
				//fmt.Println(rf.nextIndex[i])
				if args.PrevLogIndex ==0{
					args.PrevLogTerm = -1
				}else{
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				args.Entries = rf.log[args.PrevLogIndex:]
				//fmt.Println("a",len(args.Entries))
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				if rf.state!=2{
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				if rf.killed(){
					return
				}
				//fmt.Println(reply.Success)
				success = reply.Success
				time.Sleep(10 * time.Millisecond)
			}
			//fmt.Println(i,rf.nextIndex[i])
		}else{
			rf.mu.Unlock()	
		}
	}
}
func (rf *Raft) updateN() {
	for !rf.killed(){
		rf.mu.Lock()
		if rf.state==2{
			N := rf.commitIndex+1
			for N<=len(rf.log){
				if rf.log[N-1].Term!=rf.currentTerm{
					N++
					continue
				}
				sum := 1
				for i:=0; i<len(rf.peers); i++{
					if rf.me==i{
						continue
					}
					if rf.matchIndex[i]>=N{
						sum++
					}
				}
				if(sum>(len(rf.peers)/2)){
					rf.commitIndex = N
					//fmt.Println("leader",rf.commitIndex,len(rf.log))
					//fmt.Println("commitindex:",rf.commitIndex)
				}else{
					break
				}
				N++
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	
}
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.iskilled <- true
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
/*
func (rf *Raft) heartbeats(){
	for rf.killed() == false {
		if rf.state ==2{
			
			for i:=0; i<len(rf.peers); i++{
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm 
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)
			}
			ms := 100
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}

}*/


func (rf *Raft) ticker() {

	for rf.killed() == false {
		//fmt.Println("w:",rf.me)
		rf.mu.Lock()
		switch rf.state {
		case 0:
			rf.mu.Unlock()
			select {
			case <-time.After(time.Duration(rand.Int63() % 150 +300) * time.Millisecond):
				rf.mu.Lock()
				rf.state = 1
				rf.mu.Unlock()
			case <- rf.heartbeat:
			case <- rf.voted:
			case <- rf.iskilled:
			}
		case 1:
			rf.mu.Unlock()
			go rf.election()
			select{
			case <-time.After(time.Duration(rand.Int63() % 150 + 300) * time.Millisecond):
				continue;
			case <-rf.voteGranted:
			case <-rf.heartbeat:
				rf.mu.Lock()
				rf.state = 0
				rf.mu.Unlock()
			case <- rf.iskilled:
			}
		case 2:
			rf.mu.Unlock()
			rf.hb()
			//fmt.Printf("a")
			select {
			case <-time.After(time.Duration(50) * time.Millisecond):
			case <- rf.heartbeat:
				rf.mu.Lock()
				rf.state = 0
				rf.mu.Unlock()
			case <- rf.voted:
			case <- rf.voteGranted:
			case <- rf.iskilled:
			}
			//fmt.Println("c")
		}
		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
	}
	//fmt.Println("kill:",rf.me)
}
func (rf *Raft) election(){
	rf.mu.Lock()
	rf.votenum = 1;
	rf.currentTerm++;
	rf.votedFor = rf.me
	rf.persist()
	//fmt.Println(len(rf.peers))
	for i:=0; i<len(rf.peers); i++{
		if i==rf.me{
			continue
		}
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log)
		if len(rf.log)==0{
			args.LastLogTerm = -1
		}else{
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.mu.Unlock()
	//fmt.Println("elec:",rf.me)

}
func (rf *Raft) hb() {
	//fmt.Println("b")
	// := time.Now()            // 开始时间
    // 从开始到当前所消耗的时间
	//fmt.Println("log",rf.me,len(rf.log))
	
	for i:=0; i<len(rf.peers); i++{

		if i==rf.me{
			continue
		}
		
		args := AppendEntriesArgs{}
		rf.mu.Lock()
		if rf.state!=2{
			break
		}
		//fmt.Println("b")
		args.Term = rf.currentTerm 
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex =  rf.nextIndex[i]-1
		
		//fmt.Println(rf.me,rf.nextIndex[i],len(rf.log))
		if args.PrevLogIndex ==0{
			args.PrevLogTerm = -1
		}else{
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		go rf.sendAppendEntries(i, &args, &reply)
		
	}
	
	//eT := time.Since(bT)   
	//fmt.Println("Run time: ", eT)
	
	//fmt.Println("hb:",rf.me)
}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeat  = make(chan bool,50)
	rf.voteGranted = make(chan bool,50)
	rf.voted = make(chan bool,50)
	rf.iskilled = make(chan bool,50)
	rf.votenum = 0
	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.lastApplied = 0
	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyfunc()
	go rf.updateN()
	//fmt.Println("connect:",rf.me)



	return rf
}
