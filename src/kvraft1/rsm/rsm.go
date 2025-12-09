package rsm

import (
	"sync"
	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int
	OpId    uint64
	Req     any
}

type OpResult struct {
	Err rpc.Err
	Val any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	nextOpId  uint64
	notifiers map[int]*Notifier
	lastAppliedOpId map[int] uint64 // ClerkId -> last applied OpId
	opresults map[uint64] OpResult // OpId -> OpResult
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		notifiers:    make(map[int]*Notifier),
		lastAppliedOpId: make(map[int]uint64),
		opresults: make(map[uint64]OpResult),
	}

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		go rsm.reader()
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	opid := atomic.AddUint64(&rsm.nextOpId, 1)
	op := Op{ClerkId: rsm.me, OpId: opid, Req: req}
	op_ref := &op

	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// wait for op to be applied or timeout
	var prevterm int
	if !rsm.is_op_applied(op_ref) {
		_, term, is_leader := rsm.rf.Start(op)
		prevterm = term
		if !is_leader{
			// not leader, garbage collect
			return rpc.ErrWrongLeader, nil
		}
		rsm.register_to_notifier(op_ref)
		rsm.wait_op_applied(op_ref)

		currentTerm, isLeader := rsm.rf.GetState()
		if !isLeader || currentTerm != prevterm {
			raft.D4Printf("isLeader: %v term changed from %d to %d after call Submit()", isLeader, prevterm, currentTerm)
			return rpc.ErrWrongLeader, nil // i'm dead, try another server.
		}
	}

	if rsm.is_op_applied(op_ref) {
		res := rsm.opresults[op.OpId]
		rsm.lastAppliedOpId[op.ClerkId] = op.OpId
		delete(rsm.opresults, op.OpId) // garbage collect
		return res.Err, res.Val
	}
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

func (rsm *RSM) reader() {
	if rsm.rf == nil {
		return
	}
	for msg := range rsm.applyCh {
		// err_msg := ""
		if msg.SnapshotValid {
			// handle snapshot：通知状态机恢复快照（StateMachine 需实现快照恢复逻辑）
			rsm.sm.Restore(msg.Snapshot)
			raft.D4Printf("[RSM-%d] applied snapshot at index %d", rsm.me, msg.SnapshotIndex)
			continue
		}

		// ignore invalid commands
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		op_ref := &op
		if !ok {
			raft.D4Printf("[rsm-%d] invalid command type %T in applyCh", rsm.me, op)
			continue
		}

		raft.D4Printf("[rsm-%d] applying type %T op %+v at index %d", rsm.me, op, op, msg.CommandIndex)

		rsm.mu.Lock()
		// 1. the command is committed, notify to apply it to state machine
		rsm.notify(op_ref)
		if !rsm.is_op_applied(op_ref) {
			val := rsm.sm.DoOp(op.Req)
			rsm.opresults[op.OpId] = OpResult{Err: rpc.OK, Val: val}
			rsm.lastAppliedOpId[op.ClerkId] = op.OpId
		}
		rsm.mu.Unlock()
	}
}
