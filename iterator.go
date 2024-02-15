package bitcask

import (
	"bitcask/index"
	"bytes"
)

// Iterator represents an iterator for the DB.
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	cfg       IteratorConfig
}

// NewIterator creates a new Iterator for the DB.
func (db *DB) NewIterator(opts IteratorConfig) *Iterator {
	return &Iterator{
		indexIter: db.index.Iterator(opts.Reverse),
		db:        db,
		cfg:       opts,
	}
}

// Rewind rewinds the Iterator to the beginning.
func (i *Iterator) Rewind() {
	// Rewind the index iterator to the beginning.
	i.indexIter.Rewind()

	// Skip to the next element in the iterator.
	i.skipToNext()
}

// Seek sets the Iterator to the first key greater than or equal to the specified key.
func (i *Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
}

// Next advances the iterator to the next element.
func (i *Iterator) Next() {
	// Advance the underlying index iterator.
	i.indexIter.Next()

	// Skip to the next element if necessary.
	i.skipToNext()
}

// Valid returns true if the Iterator is valid, false otherwise.
func (i *Iterator) Valid() bool {
	return i.indexIter.Valid()
}

// Key returns the current key of the Iterator.
func (i *Iterator) Key() []byte {
	return i.indexIter.Key()
}

// Value returns the value associated with the current key of the Iterator.
// It first retrieves the position of the value from the index iterator.
// Then it acquires a read lock on the database's mutex to ensure thread safety.
// Finally, it calls the getValueByPosition function to retrieve the value
// associated with the position.
func (i *Iterator) Value() ([]byte, error) {
	pos := i.indexIter.Value()
	i.db.mu.RLock()
	defer i.db.mu.RUnlock()
	return i.db.getValueByPosition(pos)
}

// Close closes the Iterator and releases any resources associated with it.
// It simply calls the Close method on the index iterator.
func (i *Iterator) Close() {
	i.indexIter.Close()
}

// skipToNext skips to the next key that matches the given prefix.
// It iterates over the index iterator and compares the key with the prefix.
// If a match is found, it returns. If no match is found, it continues iterating.
func (i *Iterator) skipToNext() {
	prefixLen := len(i.cfg.Prefix)
	// If there is no prefix, skip to the next key.
	if prefixLen == 0 {
		return
	}
	// Iterate over the index iterator.
	for ; i.indexIter.Valid(); i.indexIter.Next() {
		key := i.indexIter.Key()
		// If the key matches the prefix, return.
		if prefixLen <= len(key) && bytes.Compare(i.cfg.Prefix, key[:prefixLen]) == 0 {
			return
		}
	}
}
