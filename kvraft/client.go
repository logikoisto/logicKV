package raftkv

import (
	"crypto/rand"
	"logicKV/labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []labrpc.Client
	// You will have to modify this struct.

	clientID int64
	reqID    int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []labrpc.Client) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.clientID = nrand()
	ck.reqID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	var args GetArgs
	args.Key = key
	args.ClientID = ck.clientID

	ck.mu.Lock()
	args.ReqID = ck.reqID
	ck.reqID++
	ck.mu.Unlock()

	for {
		for _, v := range ck.servers {
			var reply GetReply
			ok := v.Call("RaftKV.Get", &args, &reply)
			//raft.PrintLog("ClientGet: " + ck.String() + " ;; " + key + " ;; " + reply.String())
			if ok && !reply.WrongLeader {
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.clientID

	ck.mu.Lock()
	args.ReqID = ck.reqID
	ck.reqID++
	ck.mu.Unlock()

	for {
		for _, v := range ck.servers {
			var reply PutAppendReply
			ok := v.Call("RaftKV.PutAppend", &args, &reply)
			//raft.PrintLog("ClientPutAppend: " + ck.String() + " ;; " + key + " ;; " + value + " ;; " + op + " ;; " + reply.String())
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
