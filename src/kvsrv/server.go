package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	Smap map[string]string
	Id sync.Map

	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	name, ok := kv.Smap[args.Key] 
	if ok{
		reply.Value = name
	}else{
		reply.Value = ""
	}
	kv.mu.Unlock()
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	
	_,ok := kv.Id.Load(args.Id)
	//fmt.Println(args.Id)
	if ok{
		return
	}
	kv.mu.Lock()
	kv.Smap[args.Key] = args.Value
	kv.mu.Unlock()
	kv.Id.Store(args.Id,1)
	
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Println(args.Id)
	_,ok := kv.Id.Load(args.Id)
	
	if ok{
		kv.mu.Lock()
		reply.Value = kv.Smap[args.Key]
		kv.mu.Unlock()
		return 
	}
	kv.mu.Lock()
	reply.Value = kv.Smap[args.Key]
	kv.Smap[args.Key] = kv.Smap[args.Key]+args.Value
	kv.mu.Unlock()
	
	kv.Id.Store(args.Id,1)
	// Your code here.
}
func (kv *KVServer) Report(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Println(args.Id)
	
	kv.Id.Delete(args.Id)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.Smap = make(map[string]string)
	// You may need inistialization code here.

	return kv
}
