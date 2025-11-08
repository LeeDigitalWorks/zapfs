package index

import (
	"bytes"
	"encoding/gob"
	"os"

	"zapfs/pkg/logger"
	"zapfs/pkg/utils"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDBIndexer[K comparable, V any] struct {
	db         *leveldb.DB
	dbDir      string
	options    *opt.Options
	keyToBytes func(K) []byte
	bytesToKey func([]byte) (K, error)

	// Write options for different durability levels
	writeOpts     *opt.WriteOptions // Normal writes (buffered)
	writeOptsSync *opt.WriteOptions // Durable writes (fsync)
}

// Serialize struct to bytes
func serialize[T any](v T) ([]byte, error) {
	buf := utils.SyncPoolGetBuffer()
	if err := gob.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize bytes to struct
func deserialize[T any](data []byte) (T, error) {
	var v T
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&v)
	return v, err
}

func NewLevelDBIndexer[K comparable, V any](
	dbDir string,
	opts *opt.Options,
	keyToBytes func(K) []byte,
	bytesToKey func([]byte) (K, error)) (Indexer[K, V], error) {
	m := &LevelDBIndexer[K, V]{
		dbDir:         dbDir,
		options:       opts,
		keyToBytes:    keyToBytes,
		bytesToKey:    bytesToKey,
		writeOpts:     &opt.WriteOptions{Sync: false}, // Buffered writes (fast)
		writeOptsSync: &opt.WriteOptions{Sync: true},  // Durable writes (fsync)
	}
	db, err := leveldb.OpenFile(dbDir, opts)
	if err != nil && !errors.IsCorrupted(err) {
		return nil, err
	}
	if errors.IsCorrupted(err) {
		db, err = leveldb.RecoverFile(dbDir, opts)
		if err != nil {
			return nil, err
		}
	}
	m.db = db
	return m, nil
}

func (m *LevelDBIndexer[K, V]) Put(key K, value V) error {
	data, err := serialize(value)
	if err != nil {
		return err
	}
	return m.db.Put(m.keyToBytes(key), data, m.writeOpts)
}

// PutSync writes a key-value pair with fsync for durability.
// Use this for critical operations like RefCount updates.
func (m *LevelDBIndexer[K, V]) PutSync(key K, value V) error {
	data, err := serialize(value)
	if err != nil {
		return err
	}
	return m.db.Put(m.keyToBytes(key), data, m.writeOptsSync)
}

func (m *LevelDBIndexer[K, V]) Get(key K) (V, error) {
	data, err := m.db.Get(m.keyToBytes(key), nil)
	if err != nil {
		var zero V
		return zero, err
	}
	v, err := deserialize[V](data)
	if err != nil {
		var zero V
		return zero, err
	}
	return v, nil
}

func (m *LevelDBIndexer[K, V]) Delete(key K) error {
	return m.db.Delete(m.keyToBytes(key), m.writeOpts)
}

// DeleteSync deletes a key with fsync for durability.
func (m *LevelDBIndexer[K, V]) DeleteSync(key K) error {
	return m.db.Delete(m.keyToBytes(key), m.writeOptsSync)
}

func (m *LevelDBIndexer[K, V]) Close() error {
	if err := m.db.Close(); err != nil {
		return err
	}
	return nil
}

func (m *LevelDBIndexer[K, V]) Iterate(f func(key K, value V) error) error {
	iter := m.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key, err := m.bytesToKey(iter.Key())
		if err != nil {
			return err
		}
		value, err := deserialize[V](iter.Value())
		if err != nil {
			return err
		}
		if err := f(key, value); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (m *LevelDBIndexer[K, V]) Stream(filter func(value V) bool) <-chan V {
	ch := make(chan V)
	go func() {
		defer close(ch)
		iter := m.db.NewIterator(nil, nil)
		defer iter.Release()

		for iter.Next() {
			valueBytes := iter.Value()
			value, err := deserialize[V](valueBytes)
			if err != nil {
				logger.Error().Err(err).Msg("failed to deserialize value in stream")
				continue
			}

			if filter == nil || filter(value) {
				ch <- value
			}
		}

		if err := iter.Error(); err != nil {
			logger.Error().Err(err).Msg("leveldb iterator error in stream")
		}
	}()
	return ch
}

// Sync forces all buffered writes to disk.
// This is useful for periodic checkpoints to limit data loss window.
func (m *LevelDBIndexer[K, V]) Sync() error {
	// LevelDB doesn't have an explicit sync method, but we can force
	// a sync by doing an empty write with Sync: true
	batch := new(leveldb.Batch)
	return m.db.Write(batch, m.writeOptsSync)
}

func (m *LevelDBIndexer[K, V]) Destroy() error {
	if err := m.Close(); err != nil {
		return err
	}
	return os.RemoveAll(m.dbDir)
}
