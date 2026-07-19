package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, Err := lk.ck.Get(lk.lockname)
		if Err == rpc.ErrNoKey {
			Err = lk.ck.Put(lk.lockname, "locked", 0)
			if Err == rpc.OK {
				return
			}
		} else if val == "" {
			Err = lk.ck.Put(lk.lockname, "locked", ver)
			if Err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	_, ver, Err := lk.ck.Get(lk.lockname)
	if Err == rpc.OK {
		lk.ck.Put(lk.lockname, "", ver)
	}
}
