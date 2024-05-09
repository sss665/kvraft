package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"github.com/sasha-s/go-deadlock"
	//"fmt"
	"strings"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Operation string
	Key string
	Value string
	Id int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type KVServer struct {
	mu      deadlock.Mutex 
	mu1 	deadlock.Mutex 
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	Smap 	map[string]string
	ch    	map[int]chan Op

	maxraftstate int // snapshot if log grows this big
	Id sync.Map

	// Your definitions here.
}
func (kv *KVServer) SGet(key string) string{
	kv.mu.Lock()
	name, ok := kv.Smap[key] 
	kv.mu.Unlock()
	//fmt.Println(kv.Smap)
	if ok{
		return name
	}else{
		return ""
	}
}
func (kv *KVServer) SPut(key string, value string){
	kv.mu.Lock()
	kv.Smap[key] = value
	kv.mu.Unlock()
	//fmt.Println(kv.Smap)
}
func (kv *KVServer) SAppend(key string, value string){
	kv.mu.Lock()
	kv.Smap[key] = kv.Smap[key]+value
	kv.mu.Unlock()
	//fmt.Println(kv.Smap)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Println("get:",args.Key,kv.me)
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	command := Op{
		Operation : "Get",
		Key : args.Key,
	}
	
	index, _, isLeader := kv.rf.Start(command)
	if isLeader{
		//fmt.Println(kv.me,"serverget:",args.Key,index,term)
		channel := make(chan Op)
		kv.mu1.Lock()
		kv.ch[index] = channel
		kv.mu1.Unlock()
		select{
		case ld := <- channel:
			if ld.Operation==command.Operation && ld.Key==command.Key{
				if(ld.Value==""){
					reply.Err = ErrNoKey
				}else{
					reply.Err = OK
					reply.Value = ld.Value
				}
			}else{
				reply.Err = ErrWrongLeader
			}
			//fmt.Println(ld.Value)
			kv.mu1.Lock()
			delete(kv.ch, index)
			kv.mu1.Unlock()
		case <-time.After(time.Duration(3)* time.Second):
			reply.Err = ErrTimeout
			kv.mu1.Lock()
			delete(kv.ch, index)
			kv.mu1.Unlock()
		}
	}else{
		reply.Err = ErrWrongLeader
	}
}

// unlike in lab 2, neither Put nor Append should return a value.
// this is already reflected in the PutAppendReply struct.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("server:",args.Key,args.Value,args.Op)
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	_,ok := kv.Id.Load(args.Id)
	if ok{
		reply.Err = OK
		return
	}
	command := Op{}
	command.Id = args.Id
	if args.Op == "Put"{
		command.Operation = "Put"
		command.Key = args.Key
		command.Value = args.Value
	}
	if args.Op == "Append"{
		command.Operation = "Append"
		command.Key = args.Key
		command.Value = args.Value
	}
	index, _, isLeader := kv.rf.Start(command)
	if isLeader{
		//fmt.Println(kv.me,"server:",args.Key,args.Value,args.Op,index,term)
		channel := make(chan Op)
		kv.mu1.Lock()
		kv.ch[index] = channel  
		kv.mu1.Unlock()
		select{
		case ld := <- channel:
			if ld.Operation==command.Operation && ld.Key==command.Key && ld.Id == command.Id{
				reply.Err = OK
			}else{
				reply.Err = ErrWrongLeader
			}
			kv.mu1.Lock()
			delete(kv.ch, index)
			kv.mu1.Unlock()
		case <-time.After(time.Duration(3) * time.Second):
			reply.Err = ErrTimeout
			kv.mu1.Lock()
			delete(kv.ch, index)
			kv.mu1.Unlock()
		}

	}else{
		reply.Err = ErrWrongLeader
	}
}
func (kv *KVServer) Report(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Println(args.Id)
	
	kv.Id.Delete(args.Id)
}
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) listen(){
	for !kv.killed(){
		am := <-kv.applyCh

		//fmt.Println("listen",kv.me,am.Command)
		command := am.Command.(Op)
		if strings.EqualFold(command.Operation, "Get") == false {
			_,ok := kv.Id.Load(command.Id)
			if ok{
				kv.mu1.Lock()
				channel, ok := kv.ch[am.CommandIndex]
				kv.mu1.Unlock()
				if ok{
					channel <- command
				}
				continue
			}
		}
		switch command.Operation{
			case "Get": 	
				command.Value = kv.SGet(command.Key)
			case "Put":
				kv.SPut(command.Key,command.Value)
			case "Append":
				kv.SAppend(command.Key,command.Value)
		}
		kv.mu1.Lock()
		channel, ok := kv.ch[am.CommandIndex]
		kv.mu1.Unlock()
		if ok{
			//fmt.Println("ok:",kv.me,command,am.CommandIndex,am.Term)
			channel <- command
		}
		if strings.EqualFold(command.Operation, "Get") == false {
			kv.Id.Store(command.Id,1)
			//fmt.Println("not ok:",kv.me,command,am.CommandIndex,am.Term)
		}
		
	}
	return
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//fmt.Println(len(servers))
	kv.Smap = make(map[string]string)
	kv.ch = make(map[int]chan Op)
	go kv.listen()
	// You may need initialization code here.

	return kv
}
