package mr
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "encoding/json"
import "io"
import "strconv"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	var mnum int
	var rnum int
	for{
		args := Args{}
	// declare a reply structure.
		reply := Reply{}

		ok := call("Coordinator.Hand", &args, &reply)
		if ok {
			
			if reply.Done1==true{
				break
			}
			if reply.Done==true{
				mnum = reply.Num
				filename := reply.Filename
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content)) 
				for i := 0; i < len(kva); i++ {
					oname := "mr-"
					oname += strconv.Itoa(reply.Num)
					oname += "-"
					oname += strconv.Itoa(ihash(kva[i].Key)%reply.Nreduce)
					ofile, err := os.OpenFile(oname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
					if err != nil {
						log.Fatalf("fcannot open %v", oname)
					}
					enc := json.NewEncoder(ofile)
					err = enc.Encode(&kva[i])
					if err != nil {
						log.Fatalf("json")
					}
				}
				args := Args{}
				reply := Reply{}
				args.Mnum = mnum
				call("Coordinator.Handlemdone", &args, &reply)
			} else {
				rnum = reply.Rnum
				kva_r := []KeyValue{}
				for i:=0;i<=reply.Len;i++{
					filename := "mr-"
					filename += strconv.Itoa(i)
					filename += "-"
					filename +=strconv.Itoa(reply.Rnum)
					if _, err := os.Stat(filename); err == nil {
						file, err := os.Open(filename)
						if err != nil {
							log.Fatalf("acannot open %v", filename)
						}
						dec := json.NewDecoder(file)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							kva_r = append(kva_r, kv)
						}
						file.Close()
					 } else {
						continue
					 }

				}
				sort.Sort(ByKey(kva_r))
				oname := "mr-out-"
				oname +=  strconv.Itoa(reply.Rnum)
				dir, _ := os.Getwd()
				f, _ := os.CreateTemp(dir,"temp")
				i := 0
				for i < len(kva_r) {
					j := i + 1
					for j < len(kva_r) && kva_r[j].Key == kva_r[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva_r[k].Value)
					}
					output := reducef(kva_r[i].Key, values)

				// this is the correct format for each line of Reduce output.
					fmt.Fprintf(f, "%v %v\n", kva_r[i].Key, output)
					i = j
				}
				f.Close()
				os.Rename(f.Name(), oname)
				args := Args{}
				reply := Reply{}
				args.Rnum = rnum
				call("Coordinator.Handlerdone", &args, &reply)
			}
			// reply.Y should be 100.
		}else {
			fmt.Printf("call failed!\n")
		}
		time.Sleep(1*time.Second)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
