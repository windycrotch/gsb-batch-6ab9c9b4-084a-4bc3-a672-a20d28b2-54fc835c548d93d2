package hls

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Variant is HLS variant that gonna be use to generate HLS master playlist
type Variant struct {
	// URL indicate the location of the variant playlist.
	// If variant located on remote server, this url should
	// contain the full url
	URL string

	// Bandwidth is an integer that is the upper bound of
	// the overall bitrate for each media file, in bits per second
	Bandwidth string

	// Resolution is display size, in pixels, at which to display
	// all of the video in the playlist
	Resolution string

	// Codecs is quoted string containing a comma-separated list of formats,
	// where each format specifies a media sample type that's present
	// in a media segment in the playlist file. Valid format identifiers are
	// those in the ISO file format name space defined by RFC 6381
	Codecs string
}

// GenerateHLSVariant will generate variants info from the given resolutions.
// The available resolutions are: 360p, 480p, 720p and 1080p.
func GenerateHLSVariant(resOptions []string, locPrefix string) ([]*Variant, error) {
	if len(resOptions) == 0 {
		return nil, errors.New("Please give at least 1 resolutions.")
	}

	var variants []*Variant

	// De-duplicate presets on first occurrence while preserving the caller
	// order. Invalid presets keep being skipped as in the historical behavior.
	seen := make(map[string]struct{}, len(resOptions))
	for _, r := range resOptions {
		c, err := getConfig(r)
		if err != nil {
			continue
		}
		if _, ok := seen[c.Name]; ok {
			continue
		}
		seen[c.Name] = struct{}{}

		url := fmt.Sprintf("%s.m3u8", c.Name)
		if locPrefix != "" {
			url = locPrefix + "/" + url
		}

		v := &Variant{
			URL:        url,
			Bandwidth:  c.Bandwidth,
			Resolution: c.Resolution,
		}

		variants = append(variants, v)
	}

	if len(variants) == 0 {
		return nil, errors.New("No valid resolutions found.")
	}

	return variants, nil
}

// renderPlaylist builds the exact master playlist text used by
// GeneratePlaylist. Keeping it in one place guarantees the multi-rendition
// flow emits the unchanged field/line format.
func renderPlaylist(variants []*Variant) string {
	// M3U Header
	data := "#EXTM3U\n"
	data += "#EXT-X-VERSION:3\n"

	// Add M3U Info for each variant
	for _, v := range variants {
		// URL & bandwidth is required,
		// if not found we will excluded them from the playlist
		if v.URL == "" || v.Bandwidth == "" {
			continue
		}

		data += "#EXT-X-STREAM-INF:"
		data += fmt.Sprintf("BANDWIDTH=%s", v.Bandwidth)
		if v.Resolution != "" {
			data += fmt.Sprintf(",RESOLUTION=%s", v.Resolution)
		}
		if v.Codecs != "" {
			data += fmt.Sprintf(",CODECS=%s", v.Codecs)
		}

		data += fmt.Sprintf("\n%s\n", v.URL)
	}

	return data
}

// GeneratePlaylist will generate playlist file from the given variants.
// Variant itself can be generate from GenerateHLSVariant() function of
// suplied by the caller.
//
// It returns the create/write/sync error (if any); callers that ignore the
// returned value keep the historical statement-style usage.
func GeneratePlaylist(variants []*Variant, targetPath, filename string) error {
	// Set default filename
	if filename == "" {
		filename = "playlist.m3u8"
	}

	return WritePlaylist(variants, filepath.Join(targetPath, filename))
}

// GeneratePlaylistE is kept as an explicitly error-returning alias of
// GeneratePlaylist for callers/tests that prefer that name.
func GeneratePlaylistE(variants []*Variant, targetPath, filename string) error {
	return GeneratePlaylist(variants, targetPath, filename)
}

// WritePlaylist writes the rendered playlist to filename, surfacing create,
// write, sync and close errors.
func WritePlaylist(variants []*Variant, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	data := []byte(renderPlaylist(variants))
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// WritePlaylistAtomic writes data to a temp file in the same directory as
// destination and renames it into place, so readers either observe the previous
// complete file or the new complete file - never a half-written playlist.
func WritePlaylistAtomic(variants []*Variant, destination string) error {
	data := []byte(renderPlaylist(variants))
	return atomicWriteFile(destination, data, 0o644)
}
