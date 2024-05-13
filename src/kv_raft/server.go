package kv_raft

import (
	"bytes"
	"hhdb/labgob"
	"hhdb/labrpc"
	"hhdb/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu           sync.Mutex
	me           int
	raft         *raft.Raft
	dead         int32 // set by Kill()
	applyCh      chan raft.ApplyMsg
	lastApplied  int
	maxraftstate int // snapshot if log grows this big

	duplicateTable map[int64]LastOperationInfo
	stateMachine   *MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
}

// StartKVServer servers[] contains the ports of the set of
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
	//kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.raft = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	// 从 snapshot 中恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

// todo: 为什么get操作也要同步？get只是读操作
func (kv *KVServer) Get(args GetArgs, reply GetReply) {
	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.raft.Start(Op{
		Key: args.Key,
		//Value:    "",
		OpType: OpGet,
		//ClientId: args.,
		//SeqId:    0,
	})
	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待日志状态机结果
	kv.mu.Lock()
	notifyChan := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case opReply := <-notifyChan:
		reply.Value = opReply.Value
		reply.Err = opReply.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 获取到结果之后，清理日志channel map，避免map无限增大
	// 这里异步清理就行
	// 加锁防止并发操作map
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 判断请求是否重复
	kv.mu.Lock()
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接返回结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.raft.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 删除通知的 channel
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// 处理 apply 任务
func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				// 取出用户的操作信息
				op := message.Command.(Op)
				var opReply *OpReply
				// todo：这个报错最近一次apply的数据目的是什么
				if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
					opReply = kv.duplicateTable[op.ClientId].Reply
				} else {
					// 将操作应用状态机中
					opReply = kv.applyStateMachine(op)
					if op.OpType != OpGet {
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将结果发送回去(只有leader才能发送，follower只需应用到状态机就行了)
				if _, isLeader := kv.raft.GetState(); isLeader {
					notifyChan := kv.getNotifyChan(message.CommandIndex)
					notifyChan <- opReply
				}

				// 判断是否需要做snapshot
				if kv.maxraftstate != -1 && kv.raft.GetRaftStateSize() >= kv.maxraftstate {
					// 状态机数据做snapshot，推送snapshot给raft模块
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid { // 如果raft模块给的消息是snapshot，则用snapshot覆盖状态机(目前想到的就是主节点给从节点复制日志时，发生截断的场景)
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(kv.stateMachine)
	enc.Encode(kv.duplicateTable)
	// 告诉raft模块，对应索引之前的日志做snapshot
	kv.raft.Snapshot(index, buf.Bytes())
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// 关闭raft模块
	kv.raft.Kill()
}

func (kv *KVServer) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *KVServer) applyStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}

	return &OpReply{
		Value: value,
		Err:   err,
	}
}

func (kv *KVServer) getNotifyChan(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}

	return kv.notifyChans[index]
}

func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	if dec.Decode(&stateMachine) != nil || dec.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.stateMachine = &stateMachine
	kv.duplicateTable = dupTable
}
