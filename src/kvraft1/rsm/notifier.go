package rsm

import (
	"sync"
	"time"
)

const maxWaitTime = time.Millisecond * 500

type Notifier struct {
	done       sync.Cond
	LatestOpId uint64
}

func (rsm *RSM) get_notifier(op *Op) *Notifier {
	if notifier, ok := rsm.notifiers[op.ClerkId]; ok {
		notifier.LatestOpId = max(notifier.LatestOpId, op.OpId)
		return notifier
	}
	return nil
}

func (rsm *RSM) get_or_create_notifier(op *Op) *Notifier {
	if notifier := rsm.get_notifier(op); notifier != nil {
		return notifier
	}
	// create a new notifier
	notifier := &Notifier{
		done:       *sync.NewCond(&rsm.mu),
		LatestOpId: op.OpId,
	}
	rsm.notifiers[op.ClerkId] = notifier
	return notifier
}

func (rsm *RSM) is_op_applied(op *Op) bool {
	last_applied_opid, ok := rsm.lastAppliedOpId[op.ClerkId]
	return ok && last_applied_opid >= op.OpId
}

func (rsm *RSM) wait_op_applied(op *Op) {
	for !rsm.is_op_applied(op) {
		if notifier := rsm.get_notifier(op); notifier != nil {
			notifier.done.Wait()
		}
	}
}

func (rsm *RSM) notify(op *Op) {
	notifier := rsm.get_notifier(op)
	if notifier == nil {
		return
	}
	if op.OpId == notifier.LatestOpId {
		delete(rsm.notifiers, op.ClerkId)
	}
	notifier.done.Broadcast()
}

func (rsm *RSM) register_to_notifier(op *Op) {
	rsm.get_or_create_notifier(op)
	go func() {
		<-time.After(maxWaitTime)
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		rsm.notify(op)
	}()
}
