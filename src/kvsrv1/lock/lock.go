package lock

import (
	// "log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	id       string
	lock_key string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck,
		id:       kvtest.RandValue(8),
		lock_key: l,
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err:= lk.ck.Get(lk.lock_key)

		if val == "" || err == rpc.ErrNoKey{
			err = lk.ck.Put(lk.lock_key, lk.id, ver)
			if err == rpc.OK {
				// log.Printf("%s gets lock\n", lk.id)
				return 
			}
		}

		// sleep for a while
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here

	for {
		val, ver, err := lk.ck.Get(lk.lock_key)
		if err != rpc.OK {
			panic("fail to check lock state\n")
		}

		if val != lk.lock_key {
			// log.Printf("try to release other's lock\n")
			time.Sleep(10 * time.Millisecond)
		}

		if val != "" {
			err = lk.ck.Put(lk.lock_key, "", ver)
			if err == rpc.OK {
				// log.Printf("%s unlock\n", lk.id)
				return 
			}
		}
	}
}
