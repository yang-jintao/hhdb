package shard_kv

import (
	"hhdb/shard_ctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, leader := kv.rf.Start(command)

	// 如果不是 Leader 的话，直接返回错误
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case message := <-notifyCh:
		reply.Value = message.Value
		reply.Err = message.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
		return
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ClientOperation:
		config := command.Data.(shard_ctrler.Config)
		return kv.applyNewConfig(config)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	default:
		panic("unknown config change type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shard_ctrler.Config) *OpReply {
	// todo: 如果不是这种情况，是不是应该令currentConfig为最新的？
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shard_ctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				//	shard 需要迁移进来
				gid := kv.currentConfig.Shards[i]
				if gid != 0 { // 这里if 是什么意思
					kv.shards[i].Status = MoveIn
				}
			}
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				// shard 需要迁移出去
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}
		}
		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{
			Value: "",
			Err:   OK,
		}
	}

	return &OpReply{
		Value: "",
		Err:   ErrWrongConfig,
	}
}

func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {
	if shardDataReply.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardDataReply.ShardData {
			shard := kv.shards[shardId]
			if shard.Status == MoveIn {
				for k, v := range shardData {
					shard.KV[k] = v
				}
				// 状态置为 GC，等待清理
				shard.Status = GC
			} else {
				break // todo: 直接退出for循环了?
			}
		}

		// 拷贝去重表数据
		for clientId, dupTable := range shardDataReply.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			if !ok || table.SeqId < dupTable.SeqId {
				kv.duplicateTable[clientId] = dupTable
			}
		}
	}

	return &OpReply{Err: ErrWrongConfig}
}
