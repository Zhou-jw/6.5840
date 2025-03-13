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
		val, ver, err := lk.ck.Get(lk.lock_key)

		// already hold lock
		if val == lk.id && err == rpc.OK {
			// log.Printf("%s already gets lock", lk.id)
			return
		}

		// no one holds lock
		if val == "" || err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.lock_key, lk.id, ver)
			if err == rpc.OK {
				// log.Printf("%s gets lock, with val: %s, ver: %d\n", lk.id, val, ver)
				return
			}
			// log.Printf("%s try lock, ver: %d, err: %v\n", lk.id, ver, err)
			// if err == ErrMaybe ,then try acquire again
		}

		// sleep for a while
		// log.Printf("%s will sleep to reacquire lock, lock_key is %s\n", lk.id, lk.lock_key)
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here

	for {
		val, ver, err := lk.ck.Get(lk.lock_key)
		if err != rpc.OK {
			panic("try to release unholded lock\n")
		}

		if val != lk.id && val != "" {
			// log.Printf("%s try to release %s's lock\n", lk.id, val)
			time.Sleep(10 * time.Millisecond)
		}

		if val != "" {
			err = lk.ck.Put(lk.lock_key, "", ver)
			if err == rpc.OK || err == rpc.ErrMaybe {
				// log.Printf("%s unlock, with val: %s, ver: %d, err: %v\n\n", lk.id, val, ver, err)
				return
			}
			// log.Printf("%s try to unlock, with val%s, err: %v\n", lk.id, val, err)
		} else {
			// log.Printf("%s unlock, with val: %s\n\n", lk.id, val)
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}
