package pipeline

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/sfluor/musig/internal/pkg/db"
	"github.com/sfluor/musig/internal/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAsset = "../../../assets/test/piano.wav"

// fingerprintSnapshot returns the stored values for each key of the given
// fingerprint, as read back from the database.
func fingerprintSnapshot(t *testing.T, database db.Database, fpr map[model.EncodedKey]model.TableValue) map[model.EncodedKey][]model.TableValue {
	t.Helper()

	keys := make([]model.EncodedKey, 0, len(fpr))
	for k := range fpr {
		keys = append(keys, k)
	}

	res, err := database.Get(keys)
	require.NoError(t, err)

	return res
}

// assertNoDuplicates checks that each key maps to a list without duplicated values
func assertNoDuplicates(t *testing.T, res map[model.EncodedKey][]model.TableValue) {
	t.Helper()

	for k, values := range res {
		seen := map[model.TableValue]bool{}
		for _, v := range values {
			require.Falsef(t, seen[v], "key %d has a duplicated value: %v", k, v)
			seen[v] = true
		}
	}
}

func TestProcessAndStoreIsIdempotent(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test.bolt")

	// Reference database: the song is loaded exactly once
	ref, err := NewDefaultPipeline(dbFile + ".ref")
	require.NoError(t, err)
	refRes, err := ref.ProcessAndStore(testAsset)
	require.NoError(t, err)
	refSnapshot := fingerprintSnapshot(t, ref.DB, refRes.Fingerprint)
	require.NoError(t, ref.DB.Close())

	dbFile = filepath.Join(t.TempDir(), "test.bolt")
	p, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)
	defer p.Close()

	var firstRes *Result
	var firstSnapshot map[model.EncodedKey][]model.TableValue

	for i := 0; i < 4; i++ {
		res, err := p.ProcessAndStore(testAsset)
		require.NoError(t, err)

		snapshot := fingerprintSnapshot(t, p.DB, res.Fingerprint)
		assertNoDuplicates(t, snapshot)

		if i == 0 {
			firstRes, firstSnapshot = res, snapshot
			assert.Equal(t, refSnapshot, snapshot, "loading once must store the same set of values")
			continue
		}

		assert.Equal(t, firstRes.SongID, res.SongID, "repeated loads must reuse the song ID")
		assert.Equal(t, firstSnapshot, snapshot, "repeated loads must not change the stored fingerprint values")
	}
}

func TestProcessAndStoreKeepsSharedKeysOfDifferentSongs(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test.bolt")

	p, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)
	defer p.Close()

	key := model.EncodedKey(4242)
	songA := model.TableValue{AnchorTimeMs: 1000, SongID: 1}
	songB := model.TableValue{AnchorTimeMs: 5000, SongID: 2}

	for i := 0; i < 3; i++ {
		require.NoError(t, p.DB.Set(map[model.EncodedKey]model.TableValue{key: songA}))
		require.NoError(t, p.DB.Set(map[model.EncodedKey]model.TableValue{key: songB}))
	}

	res, err := p.DB.Get([]model.EncodedKey{key})
	require.NoError(t, err)
	assert.ElementsMatch(t, []model.TableValue{songA, songB}, res[key])
}

func TestConcurrentProcessAndStoreOfTheSameSong(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test.bolt")

	p, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)
	defer p.Close()

	const loaders = 4
	results := make([]*Result, loaders)
	errs := make([]error, loaders)

	var wg sync.WaitGroup
	for i := 0; i < loaders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = p.ProcessAndStore(testAsset)
		}(i)
	}
	wg.Wait()

	for i := 0; i < loaders; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, results[0].SongID, results[i].SongID, "concurrent loads must reuse the same song ID")
	}

	// Only one song name must have been registered
	id, err := p.DB.GetSongID("piano")
	require.NoError(t, err)
	assert.Equal(t, results[0].SongID, id)

	// Fingerprint values must not contain duplicates
	keys := make([]model.EncodedKey, 0, len(results[0].Fingerprint))
	for k := range results[0].Fingerprint {
		keys = append(keys, k)
	}
	snapshot, err := p.DB.Get(keys)
	require.NoError(t, err)
	assertNoDuplicates(t, snapshot)
	for k, v := range results[0].Fingerprint {
		assert.Equal(t, []model.TableValue{v}, snapshot[k], "concurrent loads must not duplicate values")
	}
}

func TestProcessDoesNotWriteToTheDatabase(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test.bolt")

	p, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)

	res, err := p.Process(testAsset)
	require.NoError(t, err)
	require.NoError(t, p.DB.Close())

	// Nothing must have been stored
	check, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)
	defer check.Close()

	stored, err := check.DB.Get(func() []model.EncodedKey {
		keys := make([]model.EncodedKey, 0, len(res.Fingerprint))
		for k := range res.Fingerprint {
			keys = append(keys, k)
		}
		return keys
	}())
	require.NoError(t, err)
	assert.Empty(t, stored)

	_, err = check.DB.GetSongID(model.SongNameFromPath(testAsset))
	require.Error(t, err, "processing must not register a song")
}

func TestReadOnlyPipelineFindsSongAfterRepeatedLoads(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test.bolt")

	p, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = p.ProcessAndStore(testAsset)
		require.NoError(t, err)
	}
	require.NoError(t, p.DB.Close())

	// Read only pipeline: same query as the read command
	ro, err := NewReadOnlyPipeline(dbFile)
	require.NoError(t, err)
	defer ro.Close()

	res, err := ro.Process(testAsset)
	require.NoError(t, err)

	keys := make([]model.EncodedKey, 0, len(res.Fingerprint))
	for k := range res.Fingerprint {
		keys = append(keys, k)
	}

	stored, err := ro.DB.Get(keys)
	require.NoError(t, err)
	require.NotEmpty(t, stored)

	ids := map[uint32]bool{}
	for _, values := range stored {
		for _, v := range values {
			ids[v.SongID] = true
		}
	}
	require.Len(t, ids, 1, "only the loaded song must match")

	var songID uint32
	for id := range ids {
		songID = id
	}

	name, err := ro.DB.GetSong(songID)
	require.NoError(t, err)
	assert.Equal(t, "piano", name)

	// And the ID is the one computed when loading
	loadedID, err := ro.DB.GetSongID("piano")
	require.NoError(t, err)
	assert.Equal(t, loadedID, songID)
}

func TestReadOnlyPipelineReturnsNothingForUnknownFingerprints(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test.bolt")

	p, err := NewDefaultPipeline(dbFile)
	require.NoError(t, err)
	_, err = p.ProcessAndStore(testAsset)
	require.NoError(t, err)
	require.NoError(t, p.DB.Close())

	ro, err := NewReadOnlyPipeline(dbFile)
	require.NoError(t, err)
	defer ro.Close()

	res, err := ro.DB.Get([]model.EncodedKey{model.EncodedKey(123456789)})
	require.NoError(t, err)
	assert.Empty(t, res)

	name, err := ro.DB.GetSong(999)
	require.Error(t, err)
	assert.Empty(t, name)
}
