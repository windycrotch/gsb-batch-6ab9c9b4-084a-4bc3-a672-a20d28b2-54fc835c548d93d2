package db

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sfluor/musig/internal/pkg/model"
	"go.etcd.io/bbolt"
)

var _ Database = &BoltDB{}

// BoltDB implements the Database interface using a bolt database
type BoltDB struct {
	*bbolt.DB
	fingerprintBucket []byte
	songBucket        []byte
}

// NewBoltDB returns a new bolt database
func NewBoltDB(path string) (*BoltDB, error) {
	db, err := bbolt.Open(path, 0666, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating bolt db with path:%s", path)
	}

	boltDB := &BoltDB{
		DB:                db,
		fingerprintBucket: []byte("fingerprint"),
		songBucket:        []byte("song"),
	}

	// Create buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(boltDB.fingerprintBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(boltDB.songBucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, errors.Wrap(err, "error creating buckets")
	}

	return boltDB, nil
}

// NewBoltDBReadOnly opens an existing bolt database in read-only mode.
// No bucket is created and no write transaction is ever issued on the
// returned database, which makes it suitable for lookup only operations.
func NewBoltDBReadOnly(path string) (*BoltDB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, errors.Wrapf(err, "error opening read-only bolt db with path:%s", path)
	}

	db, err := bbolt.Open(path, 0666, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, errors.Wrapf(err, "error opening read-only bolt db with path:%s", path)
	}

	return &BoltDB{
		DB:                db,
		fingerprintBucket: []byte("fingerprint"),
		songBucket:        []byte("song"),
	}, nil
}

// Get retrieves the provided keys' values from the bolt file
func (db *BoltDB) Get(keys []model.EncodedKey) (map[model.EncodedKey][]model.TableValue, error) {
	res := make(map[model.EncodedKey][]model.TableValue, len(keys))

	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.fingerprintBucket)
		if b == nil {
			// The database has no fingerprint bucket yet: nothing to return
			return nil
		}

		for _, k := range keys {
			raw := b.Get(k.Bytes())
			if len(raw) == 0 {
				continue
			}
			val, err := model.ValuesFromBytes(raw)
			if err != nil {
				return errors.Wrapf(err, "wrong record stored: %v", raw)
			}

			res[k] = val
		}

		return nil
	})

	return res, errors.Wrap(err, "an error occurred when reading from bolt")
}

// Set stores the list of (key, value) into the bolt file.
// Values that are already stored for the given key are never appended again so
// that loading the same song multiple times is idempotent. Distinct values
// sharing the same key (typically values coming from different songs) are all
// kept.
func (db *BoltDB) Set(batch map[model.EncodedKey]model.TableValue) error {
	err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.fingerprintBucket)
		if b == nil {
			return fmt.Errorf("fingerprint bucket does not exist")
		}

		for k, v := range batch {
			rawKey := k.Bytes()

			// Note: the slice returned by Get points into the memory map, it
			// must be copied before being modified.
			existingRaw := b.Get(rawKey)
			existing, err := model.ValuesFromBytes(existingRaw)
			if err != nil {
				return errors.Wrapf(err, "wrong record stored for key %v", k)
			}

			// Rebuild the value list, dropping duplicated entries and adding
			// the new value if it is not already there. Multiple songs can
			// share the same key: only identical (song, anchor time) pairs are
			// considered duplicates.
			seen := make(map[model.TableValue]struct{}, len(existing)+1)
			merged := make([]model.TableValue, 0, len(existing)+1)
			for _, value := range existing {
				if _, ok := seen[value]; ok {
					continue
				}
				seen[value] = struct{}{}
				merged = append(merged, value)
			}
			if _, ok := seen[v]; !ok {
				merged = append(merged, v)
			}

			// Nothing to do if the stored list is already in its canonical form
			if equalValues(merged, existing) {
				continue
			}

			encoded := make([]byte, 0, len(merged)*model.TableValueSize)
			for i := range merged {
				encoded = append(encoded, merged[i].Bytes()...)
			}

			if err := b.Put(rawKey, encoded); err != nil {
				return errors.Wrapf(err, "error setting (key: %v, val: %v)", k, v)
			}
		}
		return nil
	})

	return errors.Wrap(err, "an error occurred when writing to bolt")
}

// GetSongID does a song name => songID lookup in the database
func (db *BoltDB) GetSongID(name string) (uint32, error) {
	var id uint32

	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.songBucket)
		if b == nil {
			return fmt.Errorf("could not find id for song name: %s", name)
		}

		var ok bool
		id, ok = findSongID(b, name)
		if !ok {
			return fmt.Errorf("could not find id for song name: %s", name)
		}

		return nil
	})

	return id, errors.Wrap(err, "an error occurred when reading from bolt")
}

// GetSong does a songID => song name lookup in the database
func (db *BoltDB) GetSong(songID uint32) (string, error) {
	var name string

	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.songBucket)
		if b == nil {
			return fmt.Errorf("got empty song name")
		}

		raw := b.Get(itob(songID))
		if len(raw) == 0 {
			return fmt.Errorf("got empty song name")
		}

		name = string(raw)

		return nil
	})

	return name, errors.Wrap(err, "an error occurred when reading from bolt")
}

// SetSong stores a song name in the database and returns it's song ID
// It first checks if the song is not already stored in the database.
// The lookup and the insertion are done in the same write transaction so that
// concurrent calls for the same song name all reuse the same song ID.
func (db *BoltDB) SetSong(song string) (uint32, error) {
	var songID uint32

	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(db.songBucket)
		if err != nil {
			return errors.Wrapf(err, "error creating bucket")
		}

		// First check if we haven't the song in the db already
		if id, ok := findSongID(b, song); ok {
			songID = id
			return nil
		}

		id, err := b.NextSequence()
		if err != nil {
			return errors.Wrap(err, "error getting next sequence")
		}

		songID = uint32(id)
		rawKey := itob(songID)

		return errors.Wrap(b.Put(rawKey, []byte(song)), "error setting song")
	})
	if err != nil {
		return 0, errors.Wrap(err, "an error occurred when writing to bolt")
	}

	return songID, nil
}

// findSongID looks for the ID associated with the given song name in the songs bucket
func findSongID(b *bbolt.Bucket, name string) (uint32, bool) {
	c := b.Cursor()

	// TODO: this is not really efficient, we should use a dedicated index
	// Iterate over all the song records
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(k) == 4 && string(v) == name {
			return binary.LittleEndian.Uint32(k), true
		}
	}

	return 0, false
}

// equalValues returns true if both lists contain the same values in the same order
func equalValues(a, b []model.TableValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func itob(s uint32) []byte {
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, s)
	return raw
}
