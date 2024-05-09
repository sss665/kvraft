# raft

github仓库：[raft](https://github.com/sss665/raft)。 
mit6.5840 学习笔记。  
注意：论文[raft-extended](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)中的figure 2每一句话都需要严谨实现。
## LAB2 raft
### LAB2A election
    注意两个时间：选举超时，领导者心跳间隔。实现可用go语言关键字select。 
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


