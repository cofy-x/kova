//go:build !linux || !cgo

package store

import "errors"

type lmdbResultDB struct{}

func openLMDBResultDB(_ string) (*lmdbResultDB, error) {
	return nil, errors.New("lmdb support requires linux with cgo enabled")
}

func (db *lmdbResultDB) Put(entry Entry) error {
	return errors.New("lmdb support requires linux with cgo enabled")
}

func (db *lmdbResultDB) Get(target string) (Entry, bool, error) {
	return Entry{}, false, errors.New("lmdb support requires linux with cgo enabled")
}

func (db *lmdbResultDB) All() ([]Entry, error) {
	return nil, errors.New("lmdb support requires linux with cgo enabled")
}

func (db *lmdbResultDB) Sync() error {
	return errors.New("lmdb support requires linux with cgo enabled")
}

func (db *lmdbResultDB) Close() error {
	return nil
}
