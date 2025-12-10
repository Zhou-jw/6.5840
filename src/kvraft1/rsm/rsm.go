package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1
const maxWaitTime = time.Millisecond * 1500

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me     int
	OpId   uint64
	Req    any
	OpType string // "Get", "Put", "Append"
}

type pendingOp struct {
	op    *Op
	reply any
	done  chan bool
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
	nextOpId   uint64
	pendingOps map[int]*pendingOp
	r          *rand.Rand
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
		pendingOps:   make(map[int]*pendingOp),
		nextOpId:     0,
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

func (rsm *RSM) generate_next_opid() uint64 {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Uint64()
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	opid := rsm.generate_next_opid()

	op := Op{Me: rsm.me, OpId: opid, Req: req}
	pendingOp_ref := &pendingOp{
		op:   &op,
		done: make(chan bool, 1),
	}

	index, term, is_leader := rsm.rf.Start(op)
	if !is_leader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	rsm.mu.Lock()
	if pendingOp, is_existed := rsm.pendingOps[index]; is_existed {
		select {
		case pendingOp.done <- false:
		default:
		}
	}

	rsm.pendingOps[index] = pendingOp_ref
	rsm.mu.Unlock()

	// wait for op to be applied or timeout
	err, rep := rsm.wait_op_applied(pendingOp_ref, term)

	rsm.mu.Lock()
	delete(rsm.pendingOps, index)
	rsm.mu.Unlock()

	return err, rep
}

func (rsm *RSM) wait_op_applied(pendingOp *pendingOp, term int) (rpc.Err, any) {
	timeout := time.NewTimer(maxWaitTime)
	defer timeout.Stop()
	select {
	case <-timeout.C:
	case is_done := <-pendingOp.done:
		if is_done {
			currentTerm, is_leader := rsm.rf.GetState()
			if !is_leader || currentTerm != term {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, pendingOp.reply
		}
	}
	return rpc.ErrWrongLeader, nil
	// 设置绝对超时
	// timeout := time.NewTimer(100 * time.Millisecond)
	// n := 1
	// defer timeout.Stop()
	// for {
	// 	select {
	// 	case <-timeout.C:
	// 		currentTerm, isLeader := rsm.rf.GetState()
	// 		if !isLeader || currentTerm != term{
	// 		raft.D4Printf("%d * 100ms expire time", n)
	// 			return rpc.ErrWrongLeader, nil
	// 		}
	// 		raft.D4Printf("counter %d block", n)
	// 		timeout.Reset(100 * time.Millisecond)
	// 		n++
	// 	case res := <-pendingOp.done:
	// 		if res {
	// 			return rpc.OK, pendingOp.reply // 操作成功完成
	// 		} else {
	// 			return rpc.ErrWrongLeader, nil // 操作失败，领导者变更、或者操作被覆盖、或者applyCh被关闭
	// 		}
	// 	}
	// }
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
		if !ok {
			raft.D4Printf("[rsm-%d] invalid command type %T in applyCh", rsm.me, op)
			continue
		}

		rsm.mu.Lock()
		reply := rsm.sm.DoOp(op.Req)
		if pendingOp, is_existed := rsm.pendingOps[msg.CommandIndex]; is_existed {
			if pendingOp.op.OpId == op.OpId && pendingOp.op.Me == rsm.me {
				pendingOp.reply = reply
				select {
				case pendingOp.done <- true:
				default:
				}
			} else {
				raft.D4Printf("repetitive opid %d",op.OpId)
				select {
				case pendingOp.done <- false:
				default:
				}
			}
		}

		rsm.mu.Unlock()
		raft.D4Printf("[rsm-%d] applying type %T op %+v at index %d", rsm.me, op, op, msg.CommandIndex)

	}
}
