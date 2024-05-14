package shard_ctrler

import (
	"log"
	"time"
)

// NShards The number of shards.
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func DefaultConfig() Config {
	return Config{
		Groups: make(map[int][]string),
	}
}

type OpReply struct {
	ControllerConfig Config
	Err              Err
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type QueryArgs struct {
	Num int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqId    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

const ClientRequestTimeout = 500 * time.Millisecond

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Servers  map[int][]string // new GID -> servers mappings  -- for Join
	GIDs     []int            // -- for Leave
	Shard    int              // -- for Move
	GID      int              // -- for Move
	Num      int              // desired config number -- for Query
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OperationType uint8

const (
	OpJoin OperationType = iota
	OpLeave
	OpMove
	OpQuery
)
