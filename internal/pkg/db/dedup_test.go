package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sfluor/musig/internal/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func newTestDB(t *testing.T) (*BoltDB, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.bolt")
	db, err := NewBoltDB(path)
	require.NoError(t, err)
	return db, path
}

// dumpFingerprints returns a canonical representation of the fingerprint
// bucket contents: key => list of values in their stored order.
func dumpFingerprints(t *testing.T, db *BoltDB) map[model.EncodedKey][]model.TableValue {
	t.Helper()

	res := map[model.EncodedKey][]model.TableValue{}
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.fingerprintBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if len(k) != 4 {
				return fmt.Errorf("unexpected key size: %d", len(k))
			}
			key := model.EncodedKey(uint32(k[0]) | uint32(k[1])<<8 | uint32(k[2])<<16 | uint32(k[3])<<24)
			values, err := model.ValuesFromBytes(v)
			if err != nil {
				return err
			}
			res[key] = values
			return nil
		})
	})
	require.NoError(t, err)

	return res
}

func TestSetIsIdempotent(t *testing.T) {
	db, _ := newTestDB(t)
	defer db.Close()

	batch := map[model.EncodedKey]model.TableValue{
		model.EncodedKey(1): {AnchorTimeMs: 1500, SongID: 100},
		model.EncodedKey(2): {AnchorTimeMs: 15, SongID: 42},
	}

	require.NoError(t, db.Set(batch))
	first := dumpFingerprints(t, db)

	// Loading the exact same song again must not change anything
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Set(batch))
		assert.Equal(t, first, dumpFingerprints(t, db), "repeated Set changed the stored values")
	}

	for k, values := range first {
		assert.Equal(t, []model.TableValue{batch[k]}, values)
	}
}

func TestSetKeepsValuesOfDifferentSongsUnderTheSameKey(t *testing.T) {
	db, _ := newTestDB(t)
	defer db.Close()

	key := model.EncodedKey(42)
	songA := model.TableValue{AnchorTimeMs: 1000, SongID: 1}
	songB := model.TableValue{AnchorTimeMs: 2500, SongID: 2}

	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: songA}))
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: songB}))

	// Load both songs a second time, in turn and together
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: songA}))
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: songB}))
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: songA, key: songB}))
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: songA}))

	res, err := db.Get([]model.EncodedKey{key})
	require.NoError(t, err)
	assert.ElementsMatch(t, []model.TableValue{songA, songB}, res[key])
}

func TestSetDeduplicatesAlreadyStoredDuplicates(t *testing.T) {
	db, _ := newTestDB(t)
	defer db.Close()

	key := model.EncodedKey(7)
	value := model.TableValue{AnchorTimeMs: 100, SongID: 3}
	duplicated := []byte{}
	for i := 0; i < 4; i++ {
		duplicated = append(duplicated, value.Bytes()...)
	}

	// Simulate a database written by an older version where duplicates were appended
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(db.fingerprintBucket).Put(key.Bytes(), duplicated)
	}))

	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: value}))

	res, err := db.Get([]model.EncodedKey{key})
	require.NoError(t, err)
	assert.Equal(t, []model.TableValue{value}, res[key])
}

func TestSetSongIsConcurrencySafe(t *testing.T) {
	db, _ := newTestDB(t)
	defer db.Close()

	const name = "my song !"
	const goroutines = 20

	ids := make([]uint32, goroutines)
	errs := make([]error, goroutines)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = db.SetSong(name)
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, ids[0], ids[i], "concurrent SetSong calls returned different IDs")
		assert.NotZero(t, ids[0])
	}

	// Only a single song record must exist
	count := 0
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(db.songBucket).ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	}))
	assert.Equal(t, 1, count)

	id, err := db.GetSongID(name)
	require.NoError(t, err)
	assert.Equal(t, ids[0], id)
}

func TestDataIsPersistedAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.bolt")

	db, err := NewBoltDB(path)
	require.NoError(t, err)

	key := model.EncodedKey(1234)
	value := model.TableValue{AnchorTimeMs: 6543, SongID: 0}
	id, err := db.SetSong("persisted song")
	require.NoError(t, err)
	value.SongID = id
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: value}))
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: value}))
	require.NoError(t, db.Close())

	db2, err := NewBoltDB(path)
	require.NoError(t, err)
	defer db2.Close()

	sameID, err := db2.SetSong("persisted song")
	require.NoError(t, err)
	assert.Equal(t, id, sameID, "song ID must be reused after reopening the database")

	name, err := db2.GetSong(id)
	require.NoError(t, err)
	assert.Equal(t, "persisted song", name)

	res, err := db2.Get([]model.EncodedKey{key})
	require.NoError(t, err)
	assert.Equal(t, []model.TableValue{value}, res[key])

	// A new song must get a fresh ID, not collide with the persisted one
	newID, err := db2.SetSong("another song")
	require.NoError(t, err)
	assert.NotEqual(t, id, newID)
}

func TestReadOnlyDatabaseCannotBeWritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bolt")

	db, err := NewBoltDB(path)
	require.NoError(t, err)

	key := model.EncodedKey(9)
	value := model.TableValue{AnchorTimeMs: 12, SongID: 4}
	require.NoError(t, db.Set(map[model.EncodedKey]model.TableValue{key: value}))
	require.NoError(t, db.Close())

	before, err := os.Stat(path)
	require.NoError(t, err)

	ro, err := NewBoltDBReadOnly(path)
	require.NoError(t, err)

	res, err := ro.Get([]model.EncodedKey{key})
	require.NoError(t, err)
	assert.Equal(t, []model.TableValue{value}, res[key])

	name, err := ro.GetSong(4)
	require.Error(t, err)
	assert.Empty(t, name)

	err = ro.Set(map[model.EncodedKey]model.TableValue{key: {AnchorTimeMs: 1, SongID: 5}})
	require.Error(t, err, "writing to a read-only database must fail")
	require.NoError(t, ro.Close())

	after, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, before.Size(), after.Size(), "read only usage must not modify the database file")

	// Unknown fingerprints and songs fail cleanly
	db2, err := NewBoltDBReadOnly(path)
	require.NoError(t, err)
	defer db2.Close()

	res, err = db2.Get([]model.EncodedKey{model.EncodedKey(99999)})
	require.NoError(t, err)
	assert.Empty(t, res)

	_, err = db2.GetSong(12345)
	require.Error(t, err)
}

func TestReadOnlyOnMissingFileFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.bolt")
	_, err := NewBoltDBReadOnly(path)
	require.Error(t, err)

	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "the missing database must not be created")
}
