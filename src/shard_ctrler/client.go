package shard_ctrler

import (
	"crypto/rand"
	"hhdb/labrpc"
	"math/big"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	clientId int64
	seqId    int64
}

func nRand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nRand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num}
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.Servers = servers

	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Move(shard, gid int) {
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}

}