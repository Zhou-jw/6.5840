package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVer struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	rw       sync.RWMutex
	mapinner map[string]ValueVer
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		rw:       sync.RWMutex{},
		mapinner: make(map[string]ValueVer),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.rw.RLock()
	defer kv.rw.RUnlock()

	if valueVer, ok := kv.mapinner[args.Key]; ok {
		reply.Value = valueVer.Value
		reply.Version = valueVer.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.rw.Lock()
	defer kv.rw.Unlock()

	valueVer, is_existed := kv.mapinner[args.Key]
	if !is_existed {
		switch args.Version {
		case 0:
			kv.mapinner[args.Key] = ValueVer{
				Value:   args.Value,
				Version: args.Version + 1,
			}
			reply.Err = rpc.OK
		default:
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	// key already exists, with mismatched version
	if valueVer.Version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// version match, update value
	kv.mapinner[args.Key] = ValueVer{
		Value:   args.Value,
		Version: args.Version + 1,
	}
	reply.Err = rpc.OK

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
