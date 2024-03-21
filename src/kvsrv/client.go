package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	for{
		args := GetArgs{}
		reply := GetReply{}
		args.Key = key
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok{
			return reply.Value
		}else{
			continue
		}
	}
	
	// You will have to modify this function.
	
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	id := nrand()
	for{
		args := PutAppendArgs{}
		reply := PutAppendReply{}
		args.Id = id
		args.Key = key
		args.Value = value
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok{
			req := &PutAppendArgs{}
			req.Id = id
			rsp := &PutAppendReply{}
			for !ck.server.Call("KVServer.Report", req, rsp) {
			}
			return reply.Value
		}else{
			continue
		}

	}
	
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
