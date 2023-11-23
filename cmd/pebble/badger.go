// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	badger "github.com/dgraph-io/badger/v4"
)

// Adapters for Pebble. Since the interfaces above are based on Pebble's
// interfaces, it can simply forward calls for everything.
type badgerDB struct {
	d    *badger.DB
	data bytealloc.A
}

type contextKey string

const stateKey = contextKey("badgerDB")

type badgerState struct{}

func newBadgerDB(dir string) DB {
	option := badger.DefaultOptions(dir)
	option.WithSyncWrites(!disableWAL).
		WithMemTableSize(64 << 20).
		WithNumMemtables(4).
		WithBlockCacheSize(cacheSize / 2).
		WithIndexCacheSize(cacheSize / 2)

	if verbose {
	}

	bdb, err := badger.Open(option)
	if err != nil {
		log.Fatal(err)
	}
	return &badgerDB{
		d: bdb,
	}
}

func (p *badgerDB) Flush() error {
	return p.d.Sync()
}

// methods really used by YCSB benchmark
type YCSBDB interface {
	NewIter(*pebble.IterOptions) YCSBIterator
	NewBatch() YCSBBatch
	Scan(iter YCSBIterator, key []byte, count int64, reverse bool) error
	Metrics() *pebble.Metrics
	Flush() error
}

type YCSBIterator interface {
	SeekGE(key []byte) bool
	Valid() bool
	Key() []byte
	Value() []byte
	Close() error
}

type YCSBBatch interface {
	Close() error
	Commit(opts *pebble.WriteOptions) error
	Set(key, value []byte, opts *pebble.WriteOptions) error
}

func (p *badgerDB) NewIter(opts *pebble.IterOptions) iterator {
	return bTx{b: p.d, data: p.data}
}

func (p badgerDB) NewBatch() batch {
	return bTx{w: p.d.NewWriteBatch()}
}

type bTx struct {
	w *badger.WriteBatch

	b *badger.DB

	SeekKey []byte
	valid   bool
	key     []byte
	value   []byte
	data    bytealloc.A
}

func (bt bTx) SeekGE(key []byte) bool {
	return nil == bt.b.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{})
		defer it.Close()
		it.Seek(key)
		bt.data, bt.key = bt.data.Copy(it.Item().Key())
		return it.Item().Value(func(val []byte) error {
			bt.data, bt.value = bt.data.Copy(val)
			return nil
		})
	})
}
func (bt bTx) SeekLT(key []byte) bool { return bt.SeekGE(key) }

func (bt bTx) First() bool { return true }
func (bt bTx) Last() bool  { return true }
func (bt bTx) Valid() bool { return true }
func (bt bTx) Next() bool  { return true }
func (bt bTx) Prev() bool  { return true }

func (bt bTx) Key() []byte {
	return bt.key
}

func (bt bTx) Value() []byte {
	return bt.value
}

func (bt bTx) Set(key, value []byte, opts *pebble.WriteOptions) error {
	bt.w.Set(key, value)
	if opts != nil && opts.Sync {
		return bt.w.Flush()
	}
	return nil
}

func (bt bTx) Commit(opts *pebble.WriteOptions) error {
	return bt.w.Flush()
}

func (bt bTx) Close() error {
	if bt.w != nil {
		return bt.w.Flush()
	} else if bt.b != nil {
		return bt.b.Close()
	}
	return nil
}

func (bt bTx) Delete(key []byte, opts *pebble.WriteOptions) error {
	return bt.w.Delete(key)
}

func (bt bTx) LogData(data []byte, opts *pebble.WriteOptions) error {
	return nil
}

func (p *badgerDB) Scan(iter iterator, key []byte, count int64, reverse bool) error {
	return p.d.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Reverse: reverse})
		defer it.Close()
		i := int64(0)
		for it.Seek(key); it.Valid() && i < count; it.Next() {
			p.data, _ = p.data.Copy(it.Item().Key())
			it.Item().Value(func(val []byte) error {
				p.data, _ = p.data.Copy(val)
				return nil
			})
			i++
		}
		return nil
	})
}

func (p badgerDB) Close() error {
	return p.d.Close()
}

func (p badgerDB) Commit(opts *pebble.WriteOptions) error {
	return p.d.Sync()
}

func (p badgerDB) Metrics() *pebble.Metrics {
	levels := p.d.Levels()
	m := &pebble.Metrics{
		BlockCache: pebble.CacheMetrics{
			Hits:   int64(p.d.BlockCacheMetrics().Hits()),
			Misses: int64(p.d.BlockCacheMetrics().Misses()),
		},
	}

	for i := range levels {
		m.Levels[i].NumFiles = int64(levels[i].NumTables)
		m.Levels[i].Size = levels[i].Size
		m.Levels[i].Score = levels[i].Score
	}

	return m
}
