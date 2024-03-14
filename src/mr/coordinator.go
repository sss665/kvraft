package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"



type Coordinator struct {
	// Your definitions here.
	file []string
	nreduce int
	done bool
	minfo []int
	rinfo []int
}
var mu1 sync.Mutex
var mu2 sync.Mutex
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Hand(args *Args, reply *Reply) error {
	var i,j int
	var undo,redo,undo1,redo1 bool
	
	for{
		redo = false
		redo1 = false
		undo = false
		undo1 = false
		mu1.Lock()
		for i, j = range(c.minfo){
			if j==0{
				reply.Filename = c.file[i];
				reply.Num = i
				reply.Nreduce = c.nreduce
				reply.Done = true
				reply.Done1 = false
				c.minfo[i] = 1
				undo = true
				go c.timewait(i)
				break
			}
		}
		mu1.Unlock()
		if undo{
			break;
		}
		mu1.Lock()
		for i,j = range(c.minfo){
			if(j!=2){
				redo = true
				break
			}
		}
		mu1.Unlock()
		if redo{
			time.Sleep(1*time.Second)
			continue
		}
		mu2.Lock()
		for i, j = range(c.rinfo){
			if j==0{
				reply.Len = len(c.file)
				reply.Done = false
				reply.Done1 = false
				reply.Rnum = i
				undo1 = true
				go c.timewait(i)
				break
			}
		}
		mu2.Unlock()
		if undo1{
			break
		}
		mu2.Lock()
		for i,j = range(c.rinfo){
			if(j!=2){
				redo1 = true
				break
			}
		}
		mu2.Unlock()
		if redo1{
			time.Sleep(1*time.Second)
			continue
		}
		reply.Done1 = true
		c.done = true
		break
	}
	/*if c.num<len(c.file){
		fmt.Println("a")
		c.num++
		reply.Done = false
		reply.Done1 = false
	}else if c.rnum<c.nreduce{
		fmt.Println("b")
		fmt.Println(c.rnum)
		reply.Done = false
		reply.Done1 = false
		c.rnum++
	}else{
		reply.Done1 = true
		c.done = true
	}*/
	return nil
}
func (c *Coordinator) Handlemdone(args *Args, reply *Reply) error {
	mu1.Lock()
	c.minfo[args.Mnum] = 2;
	mu1.Unlock()
	return nil
}
func (c *Coordinator) Handlerdone(args *Args, reply *Reply) error {
	mu2.Lock()
	c.rinfo[args.Rnum] = 2;
	mu2.Unlock()
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) timewait(num int) {
	time.Sleep(10*time.Second)
	if(c.minfo[num]==1){
		c.minfo[num] = 0;
	}
}
func (c *Coordinator) timewait1(num int) {
	time.Sleep(10*time.Second)
	if(c.rinfo[num]==1){
		c.rinfo[num] = 0;
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	ret = c.done


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.file = files
	c.nreduce = nReduce
	c.done = false
	c.minfo = make([]int,len(files))
	c.rinfo = make([]int,nReduce)

	// Your code here.

  
	c.server()
	return &c
}
