package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// handoverGrace bounds the wait for a previous instance to get out of the way.
// A serving instance spends it stopping ffmpeg (SIGTERM, then SIGKILL after
// 2 s) and sweeping its segments, so it comfortably exceeds RunJob's worst
// case. Anything longer than that means the old instance is wedged, and the
// handover has to fail rather than let two instances pack into one directory.
const handoverGrace = 8 * time.Second

const (
	pidfileName = "helper.pid"
	jobPidName  = "ffmpeg.pid"
)

// WritePidfile records this process and the cast token it serves. A `stop`
// carrying the same token stops this cast; one carrying a different token
// belongs to a newer cast and must leave this instance alone.
func WritePidfile(path, token string) error {
	return os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"+token+"\n"), 0o644)
}

// pidClaim is who says it owns a directory: pid to signal (0 = unknown) and
// the cast token they are serving ("" = unrecorded).
type pidClaim struct {
	pid   int
	token string
}

// readPidfile returns the claim a pidfile makes, or the zero value when the
// file is missing, unreadable or malformed. It is never authoritative on its
// own: only the directory lock proves an instance is gone.
func readPidfile(path string) pidClaim {
	b, err := os.ReadFile(path)
	if err != nil {
		return pidClaim{}
	}
	lines := strings.Split(string(b), "\n")
	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil || pid <= 0 {
		return pidClaim{}
	}
	token := ""
	if len(lines) > 1 {
		token = strings.TrimSpace(lines[1])
	}
	return pidClaim{pid: pid, token: token}
}

func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// signalStop asks pid to shut down. A missing pid (0) is a no-op, and so is
// our own pid: a record naming this process is stale bookkeeping from an
// earlier run in it, and SIGTERMing ourselves would only turn a handover into
// a deadlock.
func signalStop(pid int) {
	if pid > 0 && pid != os.Getpid() {
		syscall.Kill(pid, syscall.SIGTERM)
	}
}

// awaitDir waits until deadline for the directory lock, asking whoever holds
// it to stop along the way. onlyMatch restricts the signalling to an owner
// serving that exact cast token ("" = anyone).
//
// It re-reads the lock owner every tick rather than trusting one pidfile read:
// the instance that owns the lock is written there the moment it takes the
// lock, so this also catches one that has not written its pidfile yet.
// A nil lock and nil error means the deadline passed with someone still
// holding it.
func awaitDir(dir, token string, deadline time.Time, onlyMatch string) (*DirLock, error) {
	signalled := map[int]bool{}
	for {
		lock, err := tryLockDir(dir, token)
		if err == nil {
			return lock, nil
		}
		if !isWouldBlock(err) {
			return nil, err
		}
		if owner, ok := readLockOwner(dir); ok && owner.pid > 0 &&
			(onlyMatch == "" || owner.token == onlyMatch) && !signalled[owner.pid] {
			signalStop(owner.pid)
			signalled[owner.pid] = true
		}
		if !time.Now().Before(deadline) {
			return nil, nil
		}
		time.Sleep(pollInterval)
	}
}

// AcquireCastDir is the handover: it returns exclusive ownership of dir only
// after every previous instance has stopped writing segments there.
//
// The directory lock, not the pidfile, decides when the previous instance is
// gone. A serving instance takes the lock before its first write and releases
// it only after ffmpeg has exited and its segments are swept; the kernel drops
// it when the process dies, kill -9 included. Acquiring it is therefore exactly
// the promise the caller needs, and a stale pidfile cannot lie about it.
//
// A live previous instance is asked to stop (SIGTERM) and given the grace to
// exit. If the grace runs out the handover FAILS: it must never clean and pack
// on top of an instance still writing the same files, which is what let a new
// cast inherit the old cast's playlist, and then had that playlist deleted
// underneath the TV's fetch.
//
// The returned lock owns dir for the life of the cast; Release it last, after
// the exit sweep, so a successor cannot clean the directory while this
// instance is still shutting ffmpeg down.
func AcquireCastDir(dir, token string, grace time.Duration) (*DirLock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	pidfile := filepath.Join(dir, pidfileName)
	// No early blind signal: the pid in the pidfile could be a recycled one.
	// awaitDir signals whoever actually holds the lock, which is the live
	// instance by definition, and does so within a poll interval.
	deadline := time.Now().Add(grace)
	lock, err := awaitDir(dir, token, deadline, "")
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, fmt.Errorf("previous instance did not stop within %s; refusing to pack into %s concurrently", grace, dir)
	}
	// Nothing else to wait for: with the lock in hand, the old instance has
	// exited (the kernel dropped its lock) and stopped its ffmpeg. The old
	// instance swept before releasing the lock and removed its pidfile, so
	// anything left is that sweep's leftover or an instance that died without
	// one; both are ours to remove now, and nobody else can be writing.
	reapStaleJob(dir)
	cleanOutDir(dir)
	os.Remove(pidfile)
	return lock, nil
}

// StopCast stops the cast described by token and sweeps its files, for the
// `stop` subcommand. An empty token means "stop whatever is serving" (the
// pre-token behaviour, what a manual run gets); a non-empty one means "stop
// only that cast". A stop that reaches the helper after a newer cast has taken
// over therefore does nothing: the newer cast owns the directory, and this
// command has no business killing it or deleting the segments it is serving.
//
// It sweeps only after taking the directory lock, so even when it does stop a
// cast, a successor that acquired the directory first keeps its files.
func StopCast(dir, token string, grace time.Duration) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil // nothing was ever packed here
	}
	pidfile := filepath.Join(dir, pidfileName)
	claim := readPidfile(pidfile)
	if token != "" && claim.token != "" && claim.token != token {
		// Another cast is serving here now. Nothing of ours to stop.
		return nil
	}
	// awaitDir signals the lock's actual owner, not this pidfile pid: the pid
	// could be a recycled one, and killing a stranger is worse than a slow stop.
	lock, err := awaitDir(dir, "", time.Now().Add(grace), token)
	if err != nil {
		return err
	}
	if lock == nil {
		if owner, ok := readLockOwner(dir); ok && owner.pid > 0 && processAlive(owner.pid) {
			if token != "" && owner.token != "" && owner.token != token {
				return nil // a newer cast; the watchdogs differ, not ours to kill
			}
			return fmt.Errorf("helper (pid %d) still running in %s after %s", owner.pid, dir, grace)
		}
		return fmt.Errorf("%s is still owned after %s", dir, grace)
	}
	defer lock.Release()
	// Under the lock, re-check the claim: a newer cast may have started and
	// finished between our signal and here, leaving its pidfile behind. A
	// stale stop must not sweep the files of the cast that replaced its own.
	if newer := readPidfile(pidfile); token != "" && newer.token != "" && newer.token != token {
		return nil
	}
	reapStaleJob(dir)
	os.Remove(pidfile)
	cleanOutDir(dir)
	return nil
}

// reapStaleJob kills the ffmpeg a previous instance left behind. ffmpeg runs in
// its own process group (RunJob sets Setpgid), so the recorded pid is the group
// to signal. This only matters after an abnormal death: a helper that exits
// normally stops its job before releasing the lock.
func reapStaleJob(dir string) {
	path := filepath.Join(dir, jobPidName)
	pid := readPidfile(path).pid
	if pid > 0 && processAlive(pid) {
		syscall.Kill(-pid, syscall.SIGTERM)
		if !waitGone(pid, 2*time.Second) {
			syscall.Kill(-pid, syscall.SIGKILL)
			waitGone(pid, time.Second)
		}
	}
	os.Remove(path)
}

// waitGone polls pid until it exits or grace runs out.
func waitGone(pid int, grace time.Duration) bool {
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return true
		}
		time.Sleep(pollInterval)
	}
	return !processAlive(pid)
}
