package raftkv

import (
	"encoding/gob"
	"log"
	"logicKV/labrpc"
	"logicKV/raft"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ReqType  string
	Key      string
	Value    string
	ClientID int64
	ReqID    int
}

func (obj Op) String() string {
	return "Op: ReqType - " + obj.ReqType + ", Key - " + obj.Key + ", Value - " + obj.Value + ", Client ID - " + strconv.FormatInt(obj.ClientID, 10) + ", Req ID - " + strconv.Itoa(obj.ReqID)
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvData     map[string]string
	dupMap     map[int64]int
	commitChan map[int]chan Op
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	entry := Op{ReqType: "Get", Key: args.Key, ClientID: args.ClientID, ReqID: args.ReqID}

	ok := kv.AppendEntryToLog(entry)
	if ok {
		reply.WrongLeader = false

		kv.mu.Lock()
		val, ok := kv.kvData[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
			kv.dupMap[args.ClientID] = args.ReqID
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}

	//raft.PrintLog("ServerGet: " + kv.String() + " ;; " + args.String() + " ;; " + reply.String())
	return nil
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	entry := Op{ReqType: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, ReqID: args.ReqID}
	ok := kv.AppendEntryToLog(entry)

	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
	//raft.PrintLog("PutAppendFunc: " + args.String() + " ;; " + reply.String() + " ;; " + kv.String())
	return nil
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.commitChan[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.commitChan[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		//raft.PrintLog("AppendEntryToLog: " + op.String() + " ;; " + entry.String())
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		//raft.PrintLog("AppendEntryToLog Timeout: " + entry.String())
		return false
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []labrpc.Client, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.kvData = make(map[string]string)
	kv.dupMap = make(map[int64]int)
	kv.commitChan = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh

			cmd := msg.Command.(Op)

			kv.mu.Lock()
			v, ok := kv.dupMap[cmd.ClientID]
			//raft.PrintLog("Existing ID: " + strconv.Itoa(v) + " ;; " + cmd.String())
			if !ok || v < cmd.ReqID {
				switch cmd.ReqType {
				case "Put":
					kv.kvData[cmd.Key] = cmd.Value
				case "Append":
					kv.kvData[cmd.Key] += cmd.Value
				}
				kv.dupMap[cmd.ClientID] = cmd.ReqID
				//raft.PrintLog("PutAppend: " + kv.String())
			}

			ch, ok := kv.commitChan[msg.Index]
			if ok {
				ch <- cmd
			} else {
				kv.commitChan[msg.Index] = make(chan Op, 1)
			}
			kv.mu.Unlock()
		}
	}()

	err := rpc.Register(kv)
	if err != nil {
		log.Printf("could not register %d: %v", me, err)
	}

	err = rpc.Register(kv.rf)
	if err != nil {
		log.Printf("could not register %d: %v", me, err)
	}
	return kv
}
