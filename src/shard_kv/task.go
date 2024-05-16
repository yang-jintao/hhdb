package shard_kv

import (
	"sync"
	"time"
)

// 处理 apply 任务
func (kv *ShardKV) applyTask() {
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
				var opReply *OpReply
				raftCommand := message.Command.(RaftCommand)
				if raftCommand.CmdType == ClientOperation {
					op := raftCommand.Data.(Op)
					if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
						opReply = kv.duplicateTable[op.ClientId].Reply
					} else {
						// 将操作应用状态机中
						shardId := key2shard(op.Key)
						opReply = kv.applyToStateMachine(op, shardId)
						if op.OpType != OpGet {
							kv.duplicateTable[op.ClientId] = LastOperationInfo{
								SeqId: op.SeqId,
								Reply: opReply,
							}
						}
					}
				} else {
					kv.handleConfigChangeMessage(raftCommand)
				}

				// 将结果发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				// 判断是否需要 snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 获取当前配置
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
			kv.mu.Unlock()

			kv.ConfigCommand(RaftCommand{
				CmdType: ConfigChange,
				Data:    newConfig,
			}, &OpReply{})
		}

		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 找到需要迁移进来的 shard
			gidToShards := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					// 遍历该 Group 中每一个节点，然后从 Leader 中读取到对应的 shard 数据
					getShardArgs := ShardOperationArgs{configNum, shardIds}
					for _, server := range servers {
						var getShardReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardsData", &getShardArgs, &getShardReply)
						// 获取到了 shard 的数据，执行 shard 迁移
						if ok && getShardReply.Err == OK {
							kv.ConfigCommand(RaftCommand{ShardMigration, getShardReply}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			wg.Wait()
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			gid := kv.prevConfig.Shards[i]
			if _, ok := gidToShards[gid]; !ok {
				gidToShards[gid] = make([]int, 0)
			}
			gidToShards[gid] = append(gidToShards[gid], i)
		}
	}

	return gidToShards
}

// GetShardsData 获取 shard 数据
func (kv *ShardKV) GetShardsData(args ShardOperationArgs, reply ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 当前 Group 的配置不是所需要的
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 拷贝 shard 数据
	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].copyData()
	}

	// 拷贝去重表数据
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}
}
