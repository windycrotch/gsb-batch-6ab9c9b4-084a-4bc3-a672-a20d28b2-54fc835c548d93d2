package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func writePlaylist(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

const masterWithSubs = "#EXTM3U\n" +
	"#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"subtitle_0\",DEFAULT=NO,URI=\"index_vtt.m3u8\"\n" +
	"#EXT-X-STREAM-INF:BANDWIDTH=7000000,SUBTITLES=\"subs\"\n" +
	"index.m3u8\n"

// A no-subs cast is ready on its media playlist alone.
func TestMissingPlaylistsNoSubs(t *testing.T) {
	dir := t.TempDir()
	c := JobConfig{}
	if got := MissingPlaylists(dir, c); !slices.Equal(got, []string{"index.m3u8"}) {
		t.Fatalf("empty dir: missing = %v, want [index.m3u8]", got)
	}
	writePlaylist(t, dir, "index.m3u8", "#EXTM3U\n#EXTINF:6,\nseg_0000.m4s\n")
	if got := MissingPlaylists(dir, c); len(got) != 0 {
		t.Fatalf("index.m3u8 present: missing = %v, want none", got)
	}
}

// A subtitled cast is not ready until the master, the variant it names and
// the subtitle rendition it names are all on disk. Announcing earlier hands
// the TV a master whose URIs 404.
func TestMissingPlaylistsSubtitledNeedsEveryReferencedPlaylist(t *testing.T) {
	dir := t.TempDir()
	c := JobConfig{SubMap: "2"}

	writePlaylist(t, dir, "master.m3u8", masterWithSubs)
	got := MissingPlaylists(dir, c)
	for _, want := range []string{"index.m3u8", "index_vtt.m3u8"} {
		if !slices.Contains(got, want) {
			t.Fatalf("missing = %v, want it to include %q", got, want)
		}
	}

	writePlaylist(t, dir, "index.m3u8", "#EXTM3U\n#EXTINF:6,\nseg_0000.m4s\n")
	if got := MissingPlaylists(dir, c); !slices.Equal(got, []string{"index_vtt.m3u8"}) {
		t.Fatalf("missing = %v, want [index_vtt.m3u8] — the master is useless without the rendition", got)
	}

	writePlaylist(t, dir, "index_vtt.m3u8", "#EXTM3U\n#EXTINF:6,\nseg_0000.vtt\n")
	if got := MissingPlaylists(dir, c); len(got) != 0 {
		t.Fatalf("all referenced playlists present: missing = %v, want none", got)
	}
}

// The check follows the master's own URIs rather than fixed file names, so a
// differently-named rendition (ffmpeg's -var_stream_map can emit one) is
// still required before ready.
func TestMissingPlaylistsFollowsMasterURIs(t *testing.T) {
	dir := t.TempDir()
	c := JobConfig{SubMap: "2"}
	writePlaylist(t, dir, "master.m3u8",
		"#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"s\",NAME=\"x\",URI=\"subs_en.m3u8\"\n"+
			"#EXT-X-STREAM-INF:BANDWIDTH=1\nvariant_0.m3u8\n")
	got := MissingPlaylists(dir, c)
	for _, want := range []string{"variant_0.m3u8", "subs_en.m3u8"} {
		if !slices.Contains(got, want) {
			t.Fatalf("missing = %v, want it to include the master's own URI %q", got, want)
		}
	}
}

// hlsenc touches playlists before it writes them. An empty (or non-playlist)
// file must not count as ready: the TV would fetch it and play nothing.
func TestMissingPlaylistsRejectsEmptyAndNonPlaylistFiles(t *testing.T) {
	dir := t.TempDir()
	writePlaylist(t, dir, "index.m3u8", "")
	if got := MissingPlaylists(dir, JobConfig{}); len(got) != 1 {
		t.Fatalf("empty index.m3u8 counted as ready: missing = %v", got)
	}
	writePlaylist(t, dir, "index.m3u8", "garbage")
	if got := MissingPlaylists(dir, JobConfig{}); len(got) != 1 {
		t.Fatalf("a file that is not a playlist counted as ready: missing = %v", got)
	}
}

// ffmpeg rewrites the master in place, so a poll can catch it truncated to its
// header. The check must not mistake that for a complete master: with the
// rendition file absent it still reports the rendition missing, so the next
// 200 ms poll re-reads the grown master.
func TestMissingPlaylistsTreatsATruncatedMasterAsNotReady(t *testing.T) {
	dir := t.TempDir()
	c := JobConfig{SubMap: "2"}
	writePlaylist(t, dir, "master.m3u8", "#EXTM3U\n")
	writePlaylist(t, dir, "index.m3u8", "#EXTM3U\n#EXTINF:6,\nseg_0000.m4s\n")
	if got := MissingPlaylists(dir, c); !slices.Contains(got, "index_vtt.m3u8") {
		t.Fatalf("a truncated master without the rendition counted as ready: missing = %v", got)
	}

	os.Remove(filepath.Join(dir, "master.m3u8"))
	if got := MissingPlaylists(dir, c); !slices.Contains(got, "master.m3u8") {
		t.Fatalf("a missing master counted as ready: missing = %v", got)
	}

	// Once ffmpeg has finished the rewrite the same files are ready.
	writePlaylist(t, dir, "master.m3u8", masterWithSubs)
	writePlaylist(t, dir, "index_vtt.m3u8", "#EXTM3U\n#EXTINF:6,\nseg_0000.vtt\n")
	if got := MissingPlaylists(dir, c); len(got) != 0 {
		t.Fatalf("a complete master with its rendition is not ready: missing = %v", got)
	}
}
