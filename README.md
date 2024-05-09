# KVraft

github仓库：[raft](https://github.com/sss665/raft)。 
mit6.5840 学习笔记。  
注意：论文[raft-extended](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)中的figure 2每一句话都需要严谨实现。在实现出现bug或者卡住时可以看看[student guidence](https://thesquareplanet.com/blog/students-guide-to-raft)
## LAB3 raft
### LAB 3A leader election 
注意两个时间：选举超时，领导者心跳间隔。实现可用go语言关键字select和函数time.After。  
难点在于定时器的使用。
### LAB 3B log
Lab 3B为raft主体实现，论文的figure 2注意理解和严谨实现。我在一开始也并不严谨遵循，只是按照我的理解写的，但test通过不了，通过论文阅读和自己的思考不得不服figure 2的严谨。  
难点在于增加日志和接收日志函数要严格按figure 2实现。
### LAB 3C persistence
3C只要在3B的基础上增加persistence，但测试代码比3B要更严格，情况会更复杂，消息量会更多，主要还是考验3B代码的鲁棒性。  
难点在于优化LAB 3B使其更完善更快。
## LAB4 KVraft
### LAB4A Key/value service without snapshots
LAB4A在LAB3的基础上增加了上层的KV数据库服务端和客户端。主要难点在于网络不可信赖的情况下客户端重复发送导致的重复执行问题。  




