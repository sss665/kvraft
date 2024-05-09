package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	leader int 
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	//fmt.Println(len(servers))
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//fmt.Println(key)
	for i:=ck.leader;;i = (i+1)%len(ck.servers){
		//fmt.Println(i,"client:",key,"Get")
		args := GetArgs{}
		reply := GetReply{}
		args.Key = key
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if ok{
			//fmt.Println(reply.Err)
			switch reply.Err{
			case OK:
				ck.leader = i
				//fmt.Println("getOK:",key)
				return reply.Value
			case ErrNoKey:
				ck.leader = i
				//fmt.Println("getOK:",key)
				return ""
			case ErrWrongLeader:
				continue
			case ErrTimeout:
				continue

			}
		}
	}

	// You will have to modify this function.
	return ""
}
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	
	id := nrand()
	for i:=ck.leader;;i = (i+1)%len(ck.servers){
		//fmt.Println(i,"client:",key,"value:",value,op)
		args := PutAppendArgs{}
		reply := PutAppendReply{}
		args.Id = id
		args.Key = key
		args.Value = value
		args.Op = op
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		//fmt.Println("a")
		if ok{

			//fmt.Println(reply.Err)
			if reply.Err == ErrWrongLeader||reply.Err == ErrTimeout{
				continue
			}
			if reply.Err == OK{
				/*for j:=0;j<len(ck.servers);j++{
					req := &PutAppendArgs{}
					req.Id = id
					rsp := &PutAppendReply{}
					for !ck.servers[j].Call("KVServer.Report", req, rsp){

					}
				}
				ck.leader = i*/
				return
			}	
					//fmt.Println("sid:",i,"client:",key,"value:",value,op)
					//fmt.Println("clientOK:",key,value,op)
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
