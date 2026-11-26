package cmd

import (
	"fmt"
	"math"
	"os"

	"github.com/sfluor/musig/internal/pkg/model"
	"github.com/sfluor/musig/internal/pkg/pipeline"
	"github.com/sfluor/musig/pkg/dsp"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(readCmd)
}

// readCmd represents the read command
var readCmd = &cobra.Command{
	Use:   "read [file]",
	Short: "Read reads the given audio file trying to find it's song name",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cmdRead(args[0])
	},
}

func cmdRead(file string) {
	// Reading only looks up fingerprints, it must never write to the database
	if _, err := os.Stat(dbFile); err != nil {
		fmt.Println("Matches:")
		fmt.Println("---")
		fmt.Println("Song is: <unknown> (no match found: database is empty or does not exist)")
		return
	}

	p, err := pipeline.NewReadOnlyPipeline(dbFile)
	failIff(err, "error creating pipeline")
	defer p.Close()

	res, err := p.Process(file)
	failIff(err, "error processing file %s", file)

	// Will hold a count of songID => occurrences
	keys := make([]model.EncodedKey, 0, len(res.Fingerprint))
	sample := map[model.EncodedKey]model.TableValue{}
	// songID => points that matched
	matches := map[uint32]map[model.EncodedKey]model.TableValue{}

	for k, v := range res.Fingerprint {
		keys = append(keys, k)
		sample[k] = v
	}

	m, err := p.DB.Get(keys)
	failIff(err, "error reading fingerprints from the database")
	for key, values := range m {
		for _, val := range values {

			if _, ok := matches[val.SongID]; !ok {
				matches[val.SongID] = map[model.EncodedKey]model.TableValue{}
			}

			matches[val.SongID][key] = val
		}
	}

	// songID => correlation
	scores := map[uint32]float64{}
	for songID, points := range matches {
		score := dsp.MatchScore(sample, points)
		// A single matching point makes the correlation undefined (0/0).
		// Report it as a null score instead of NaN.
		if math.IsNaN(score) || math.IsInf(score, 0) {
			score = 0
		}
		scores[songID] = score
	}

	var song string
	var max float64
	fmt.Println("Matches:")
	for id, score := range scores {
		name, err := p.DB.GetSong(id)
		if err != nil {
			// The fingerprint table references a song that is not stored anymore,
			// do not fail the whole lookup because of it
			name = fmt.Sprintf("<unknown song id: %d>", id)
		}
		fmt.Printf("\t- %s, score: %f\n", name, score)
		if score > max {
			song, max = name, score
		}
	}

	fmt.Println("---")
	if song == "" || max <= 0 {
		fmt.Println("Song is: <unknown> (no match found in the database)")
		return
	}
	fmt.Printf("Song is: %s (score: %f)\n", song, max)
}
