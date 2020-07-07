package raftkv

import "strconv"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientID int64
	ReqID    int
}

func (obj PutAppendArgs) String() string {
	return "PutAppendArgs: Key - " + obj.Key + ", Value - " + obj.Value + ", Op - " + obj.Op + ", Client ID - " + strconv.FormatInt(obj.ClientID, 10) + ", Req ID - " + strconv.Itoa(obj.ReqID)
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

func (obj PutAppendReply) String() string {
	return "PutAppendReply: Wrong Leader - " + strconv.FormatBool(obj.WrongLeader)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientID int64
	ReqID    int
}

func (obj GetArgs) String() string {
	return "PutAppendReply: Key - " + obj.Key + ", Client ID - " + strconv.FormatInt(obj.ClientID, 10) + ", Req ID - " + strconv.Itoa(obj.ReqID)
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

func (obj GetReply) String() string {
	return "PutAppendReply: Wrong Leader - " + strconv.FormatBool(obj.WrongLeader) + ", Value - " + obj.Value
}
