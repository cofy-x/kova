//go:build linux && cgo

package store

/*
#cgo LDFLAGS: -llmdb
#include <stdlib.h>
#include <string.h>
#include <lmdb.h>

static int go_mdb_get(MDB_txn *txn, MDB_dbi dbi, const char *key, size_t klen, MDB_val *data) {
    MDB_val keyv;
    keyv.mv_size = klen;
    keyv.mv_data = (void *)key;
    return mdb_get(txn, dbi, &keyv, data);
}

static int go_mdb_put(MDB_txn *txn, MDB_dbi dbi, const char *key, size_t klen, const char *val, size_t vlen) {
    MDB_val keyv;
    MDB_val datav;
    keyv.mv_size = klen;
    keyv.mv_data = (void *)key;
    datav.mv_size = vlen;
    datav.mv_data = (void *)val;
    return mdb_put(txn, dbi, &keyv, &datav, 0);
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const lmdbMapSize = 1 << 30

type lmdbResultDB struct {
	path    string
	env     *C.MDB_env
	dbi     C.MDB_dbi
	writeMu sync.Mutex
	closed  atomic.Bool
}

func openLMDBResultDB(path string) (*lmdbResultDB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	var env *C.MDB_env
	if rc := C.mdb_env_create(&env); rc != 0 {
		return nil, lmdbError("create environment", rc)
	}

	cleanup := func() {
		if env != nil {
			C.mdb_env_close(env)
		}
	}

	if rc := C.mdb_env_set_maxdbs(env, 1); rc != 0 {
		cleanup()
		return nil, lmdbError("set max dbs", rc)
	}
	if rc := C.mdb_env_set_mapsize(env, C.size_t(lmdbMapSize)); rc != 0 {
		cleanup()
		return nil, lmdbError("set map size", rc)
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if rc := C.mdb_env_open(env, cPath, C.uint(C.MDB_NOSUBDIR|C.MDB_NOTLS), 0o644); rc != 0 {
		cleanup()
		return nil, lmdbError("open environment", rc)
	}

	var txn *C.MDB_txn
	if rc := C.mdb_txn_begin(env, nil, 0, &txn); rc != 0 {
		cleanup()
		return nil, lmdbError("begin open transaction", rc)
	}

	var dbi C.MDB_dbi
	cName := C.CString("results")
	defer C.free(unsafe.Pointer(cName))
	if rc := C.mdb_dbi_open(txn, cName, C.MDB_CREATE, &dbi); rc != 0 {
		C.mdb_txn_abort(txn)
		cleanup()
		return nil, lmdbError("open database", rc)
	}
	if rc := C.mdb_txn_commit(txn); rc != 0 {
		cleanup()
		return nil, lmdbError("commit open transaction", rc)
	}

	return &lmdbResultDB{
		path: path,
		env:  env,
		dbi:  dbi,
	}, nil
}

func (db *lmdbResultDB) Put(entry Entry) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	var txn *C.MDB_txn
	if rc := C.mdb_txn_begin(db.env, nil, 0, &txn); rc != 0 {
		return lmdbError("begin write transaction", rc)
	}

	keyBytes := []byte(entry.Target)
	keyPtr := C.CBytes(keyBytes)
	defer C.free(keyPtr)
	valuePtr := C.CBytes(payload)
	defer C.free(valuePtr)

	if rc := C.go_mdb_put(txn, db.dbi, (*C.char)(keyPtr), C.size_t(len(keyBytes)), (*C.char)(valuePtr), C.size_t(len(payload))); rc != 0 {
		C.mdb_txn_abort(txn)
		return lmdbError("put entry", rc)
	}
	if rc := C.mdb_txn_commit(txn); rc != 0 {
		return lmdbError("commit write transaction", rc)
	}
	return nil
}

func (db *lmdbResultDB) Get(target string) (Entry, bool, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var txn *C.MDB_txn
	if rc := C.mdb_txn_begin(db.env, nil, C.MDB_RDONLY, &txn); rc != 0 {
		return Entry{}, false, lmdbError("begin read transaction", rc)
	}
	defer C.mdb_txn_abort(txn)

	keyBytes := []byte(target)
	keyPtr := C.CBytes(keyBytes)
	defer C.free(keyPtr)

	var value C.MDB_val
	if rc := C.go_mdb_get(txn, db.dbi, (*C.char)(keyPtr), C.size_t(len(keyBytes)), &value); rc != 0 {
		if rc == C.MDB_NOTFOUND {
			return Entry{}, false, nil
		}
		return Entry{}, false, lmdbError("get entry", rc)
	}

	entryBytes := C.GoBytes(value.mv_data, C.int(value.mv_size))
	var entry Entry
	if err := json.Unmarshal(entryBytes, &entry); err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

func (db *lmdbResultDB) All() ([]Entry, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var txn *C.MDB_txn
	if rc := C.mdb_txn_begin(db.env, nil, C.MDB_RDONLY, &txn); rc != 0 {
		return nil, lmdbError("begin scan transaction", rc)
	}
	defer C.mdb_txn_abort(txn)

	var cursor *C.MDB_cursor
	if rc := C.mdb_cursor_open(txn, db.dbi, &cursor); rc != 0 {
		return nil, lmdbError("open cursor", rc)
	}
	defer C.mdb_cursor_close(cursor)

	entries := make([]Entry, 0)
	var key C.MDB_val
	var value C.MDB_val
	for rc := C.mdb_cursor_get(cursor, &key, &value, C.MDB_FIRST); ; rc = C.mdb_cursor_get(cursor, &key, &value, C.MDB_NEXT) {
		if rc == C.MDB_NOTFOUND {
			break
		}
		if rc != 0 {
			return nil, lmdbError("iterate cursor", rc)
		}

		entryBytes := C.GoBytes(value.mv_data, C.int(value.mv_size))
		var entry Entry
		if err := json.Unmarshal(entryBytes, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (db *lmdbResultDB) Sync() error {
	if db == nil || db.closed.Load() {
		return nil
	}
	if rc := C.mdb_env_sync(db.env, 1); rc != 0 {
		return lmdbError("sync environment", rc)
	}
	return nil
}

func (db *lmdbResultDB) Close() error {
	if db == nil || !db.closed.CompareAndSwap(false, true) {
		return nil
	}
	if db.env != nil {
		C.mdb_dbi_close(db.env, db.dbi)
		C.mdb_env_close(db.env)
		db.env = nil
	}
	return nil
}

func lmdbError(action string, rc C.int) error {
	return fmt.Errorf("%s: %s", action, C.GoString(C.mdb_strerror(rc)))
}
