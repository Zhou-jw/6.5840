package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

const retryInterval = 100 * time.Millisecond

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	nextOpId int
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, nextOpId: 1}
	// You'll have to add code here.
	return ck
}

// func (ck *Clerk) next_op_id() uint64 {
// 	ck.nextOpId++
// 	return uint64(ck.nextOpId - 1)
// }

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{
		// ClerkId: ck.leaderId, 
		// OpId: ck.next_op_id(), 
		Key: key,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			serverId := (ck.leaderId + i) % len(ck.servers)

			reply := rpc.GetReply{}
			if ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply); ok {
				switch reply.Err {
				case rpc.ErrWrongLeader:
					continue
				case rpc.ErrNoKey:
					ck.leaderId = serverId
					tester.D4Printf("get key:%v, no such key", key)
					return "", 0, reply.Err
				case rpc.OK:
					ck.leaderId = serverId
					tester.D4Printf("get key:%v, value: %v, version: %v", key, reply.Value, reply.Version)
					return reply.Value, reply.Version, reply.Err
				}
			}
		}
		time.Sleep(retryInterval)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	resent := false
	for {
		for i := 0; i < len(ck.servers); i++ {
			serverId := (ck.leaderId + i) % len(ck.servers)
			reply := rpc.PutReply{}
			if ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply); ok {
				switch reply.Err {
				case rpc.OK:
					ck.leaderId = serverId
					tester.D4Printf("successfully put key:%s, value: %s, ver: %d", key, value, version)
				case rpc.ErrVersion:
					if resent {
						return rpc.ErrMaybe
					}
				case rpc.ErrWrongLeader:
					continue
				}
				return reply.Err
			} 
		}
		resent = true
		time.Sleep(retryInterval)
	}
	// return rpc.ErrNoKey
}
