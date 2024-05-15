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
	default:
		panic("unknown config change type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shard_ctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shard_ctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				//	shard 需要迁移进来 todo
			}
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				// shard 需要迁移出去 todo
			}
		}
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
