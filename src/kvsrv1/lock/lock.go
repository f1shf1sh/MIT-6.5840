package lock

import (
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
	lockname string
	id       string
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
	lk.id = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.lockname, lk.id, 0)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				v, _, err := lk.ck.Get(lk.lockname)
				if err == rpc.OK {
					if v == lk.id {
						return
					}
				}
			}
		} else if val == "" {
			err = lk.ck.Put(lk.lockname, lk.id, ver)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				v, _, err := lk.ck.Get(lk.lockname)
				if err == rpc.OK {
					if v == lk.id {
						return
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			continue
		}

		if val != lk.id {
			return
		}

		err = lk.ck.Put(lk.lockname, "", ver)
		if err == rpc.OK || err == rpc.ErrMaybe {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
