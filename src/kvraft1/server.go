package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type ValueVer struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu       sync.Mutex
	db_inner map[string]ValueVer
}

type KVReq struct {
	OpType  string
	Key     string
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kvreq, ok := req.(KVReq)
	if !ok {
		tester.D4Printf("invalid op type in DoOp")
		return nil
	}

	switch kvreq.OpType {
	case "Get":
		rep := kv.do_get(kvreq.Key)
		return rep
	case "Put":
		rep := kv.do_put(kvreq.Key, kvreq.Value, kvreq.Version)
		return rep
	default:
		tester.D4Printf("unknown op type in DoOp: %T", req)
	}
	return nil
}

func (kv *KVServer) do_get(key string) rpc.GetReply {
	reply := rpc.GetReply{}

	if valueVer, ok := kv.db_inner[key]; ok {
		reply.Value = valueVer.Value
		reply.Version = valueVer.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return reply
}

func (kv *KVServer) do_put(key string, value string, version rpc.Tversion) rpc.PutReply {
	reply := rpc.PutReply{}
	valueVer, is_existed := kv.db_inner[key]

	if !is_existed {
		switch version {
		case 0:
			kv.db_inner[key] = ValueVer{
				Value:   value,
				Version: version + 1,
			}
			reply.Err = rpc.OK
			tester.D4Printf("insert key:%s, value:%s, version: %d to %d", key, value, version, kv.db_inner[key].Version)
		default:
			reply.Err = rpc.ErrNoKey
		}
		return reply
	}

	if version != valueVer.Version {
		reply.Err = rpc.ErrVersion
		return reply
	}

	kv.db_inner[key] = ValueVer{
		Value:   value,
		Version: version + 1,
	}
	tester.D4Printf("update key:%s, value:%s, version: %d to %d", key, value, version, kv.db_inner[key].Version)
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	kvreq := KVReq{
		OpType: "Get",
		Key:    args.Key,
	}

	err, rep := kv.rsm.Submit(kvreq)
	if err != rpc.OK {
		tester.D4Printf("fail to submit get req: %v, got res: %v", kvreq, rep)
		return
	}

	get_reply, ok := rep.(rpc.GetReply)
	if !ok {
		tester.D4Printf("invalid reply type in Get: %T", rep)
		return
	}

	reply.Value = get_reply.Value
	reply.Version = get_reply.Version
	reply.Err = get_reply.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	kvreq := KVReq{
		OpType: "Put",
		Key:    args.Key,
		Value:  args.Value,
		Version: args.Version,
	}

	err, rep := kv.rsm.Submit(kvreq)
	switch err {
	case rpc.OK:
		put_reply, ok := rep.(rpc.PutReply)
		if !ok {
			tester.D4Printf("invalid reply type in Get: %T", rep)
			return
		}
		reply.Err = put_reply.Err
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
	}

	tester.D4Printf("Err %v: fail to submit put req: %v, got res: %v", err, kvreq, rep)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(KVReq{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.db_inner = make(map[string]ValueVer)
	return []tester.IService{kv, kv.rsm.Raft()}
}
