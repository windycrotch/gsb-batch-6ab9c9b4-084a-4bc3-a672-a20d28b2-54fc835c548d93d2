package main

import (
	"os"
	"path/filepath"
	"strings"
)

// MissingPlaylists reports which of this cast's playlists are not on disk yet.
// The helper advertises the stream only when this is empty.
//
// "The playlist exists" is not enough. With subtitles the TV is handed
// master.m3u8, which names a variant playlist and a subtitle rendition
// playlist; the TV fetches those itself and 404s on either if the helper
// announces the master before they have landed. So the check follows the
// master's own references instead of assuming file names: the variant line
// (ffmpeg's, or the one the server synthesizes from index.m3u8) and every
// EXT-X-MEDIA URI, which is where the subtitle rendition lives. A file that
// exists but is empty or does not start with #EXTM3U is not counted — hlsenc
// touches playlists before writing them, and an empty one plays nothing.
func MissingPlaylists(dir string, c JobConfig) []string {
	var missing []string
	for _, name := range c.referencedPlaylists(dir) {
		if !playlistUsable(filepath.Join(dir, name)) {
			missing = append(missing, name)
		}
	}
	return missing
}

// referencedPlaylists is what this cast's served playlist will point the TV at:
// the playlist itself, plus — for a subtitled cast — the variant and the
// subtitle rendition named inside the master on disk. A master without a
// variant line still gets one from EnsureVariant at serve time, and it names
// index.m3u8, so that file is required either way.
//
// A subtitled cast's master that references no subtitle rendition is not
// counted as usable at all (the caller sees it as missing): ffmpeg rewrites the
// master in place, so a poll can catch it truncated to its header, and
// announcing then would hand the TV the partial master's (absent) subtitle
// track. Waiting is correct — the rendition lands a moment later, or the cast
// is genuinely broken and the 120 s deadline says so.
func (c JobConfig) referencedPlaylists(dir string) []string {
	if !c.HasSub() {
		return []string{"index.m3u8"}
	}
	names := []string{"master.m3u8", "index.m3u8"}
	b, err := os.ReadFile(filepath.Join(dir, "master.m3u8"))
	if err != nil {
		return names
	}
	sawSubRendition := false
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#EXT-X-MEDIA:") && strings.Contains(line, "TYPE=SUBTITLES") {
			if uri := playlistMediaURI(line); uri != "" && !strings.ContainsAny(uri, "?#") {
				names = append(names, uri)
				sawSubRendition = true
			}
			continue
		}
		// A non-comment, non-empty line after #EXT-X-STREAM-INF is the variant
		// URI. index.m3u8 is already covered.
		if line != "" && !strings.HasPrefix(line, "#") && line != "index.m3u8" && !containsStr(names, line) {
			names = append(names, line)
		}
	}
	if !sawSubRendition {
		// The master is incomplete (caught mid-rewrite) or does not describe the
		// subtitles this cast asked for. Either way the rendition is required.
		names = append(names, "index_vtt.m3u8")
	}
	return names
}

// playlistMediaURI pulls the URI="..." value out of an EXT-X-MEDIA line.
func playlistMediaURI(line string) string {
	const k = `URI="`
	i := strings.Index(line, k)
	if i < 0 {
		return ""
	}
	rest := line[i+len(k):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func playlistUsable(path string) bool {
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() || fi.Size() == 0 {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	buf := make([]byte, 7) // len("#EXTM3U")
	n, _ := f.Read(buf)
	return strings.HasPrefix(string(buf[:n]), "#EXTM3U")
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
