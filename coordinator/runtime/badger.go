package runtime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	badgerdb "github.com/dgraph-io/badger/v4"
)

const (
	badgerKeyCheckpoint = "checkpoint/current"
	badgerWALPrefix     = "wal/"
)

type BadgerStore struct {
	db        *badgerdb.DB
	closeOnce sync.Once
	closeErr  error
}

func OpenBadgerStore(path string) (*BadgerStore, error) {
	opts := badgerdb.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = true
	opts.Dir = path
	opts.ValueDir = path

	db, err := badgerdb.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("err in badger.Open: %w", err)
	}
	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
		if s.closeErr != nil {
			s.closeErr = fmt.Errorf("err in s.db.Close: %w", s.closeErr)
		}
	})
	return s.closeErr
}

func (s *BadgerStore) LoadLatestCheckpoint(_ context.Context) (Checkpoint, bool, error) {
	var checkpoint Checkpoint
	found := false
	err := s.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get([]byte(badgerKeyCheckpoint))
		if err == badgerdb.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("err in txn.Get(checkpoint): %w", err)
		}
		found = true
		return item.Value(func(val []byte) error {
			if err := json.Unmarshal(val, &checkpoint); err != nil {
				return fmt.Errorf("err in json.Unmarshal(checkpoint): %w", err)
			}
			return nil
		})
	})
	if err != nil {
		return Checkpoint{}, false, err
	}
	if !found {
		return Checkpoint{}, false, nil
	}
	return checkpoint, true, nil
}

func (s *BadgerStore) LoadWAL(_ context.Context, afterIndex uint64) ([]LogRecord, error) {
	records := []LogRecord{}
	err := s.db.View(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(badgerWALPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			index, ok := parseWALKey(string(item.Key()))
			if !ok || index <= afterIndex {
				continue
			}
			var record LogRecord
			if err := item.Value(func(val []byte) error {
				if err := json.Unmarshal(val, &record); err != nil {
					return fmt.Errorf("err in json.Unmarshal(wal): %w", err)
				}
				return nil
			}); err != nil {
				return err
			}
			records = append(records, cloneLogRecord(record))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (s *BadgerStore) AppendWAL(_ context.Context, record LogRecord) error {
	encoded, err := json.Marshal(cloneLogRecord(record))
	if err != nil {
		return fmt.Errorf("err in json.Marshal(record): %w", err)
	}
	return s.db.Update(func(txn *badgerdb.Txn) error {
		if err := txn.Set(walKey(record.Index), encoded); err != nil {
			return fmt.Errorf("err in txn.Set(wal): %w", err)
		}
		return nil
	})
}

func (s *BadgerStore) SaveCheckpoint(_ context.Context, checkpoint Checkpoint) error {
	encoded, err := json.Marshal(cloneCheckpoint(checkpoint))
	if err != nil {
		return fmt.Errorf("err in json.Marshal(checkpoint): %w", err)
	}
	return s.db.Update(func(txn *badgerdb.Txn) error {
		if err := txn.Set([]byte(badgerKeyCheckpoint), encoded); err != nil {
			return fmt.Errorf("err in txn.Set(checkpoint): %w", err)
		}
		return nil
	})
}

func (s *BadgerStore) TruncateWAL(_ context.Context, throughIndex uint64) error {
	return s.db.Update(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(badgerWALPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			index, ok := parseWALKey(string(item.Key()))
			if !ok || index > throughIndex {
				continue
			}
			if err := txn.Delete(item.KeyCopy(nil)); err != nil {
				return fmt.Errorf("err in txn.Delete(wal): %w", err)
			}
		}
		return nil
	})
}

func walKey(index uint64) []byte {
	key := make([]byte, len(badgerWALPrefix)+8)
	copy(key, badgerWALPrefix)
	binary.BigEndian.PutUint64(key[len(badgerWALPrefix):], index)
	return key
}

func parseWALKey(key string) (uint64, bool) {
	if len(key) != len(badgerWALPrefix)+8 || key[:len(badgerWALPrefix)] != badgerWALPrefix {
		return 0, false
	}
	return binary.BigEndian.Uint64([]byte(key[len(badgerWALPrefix):])), true
}
