package main

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// lockName is the advisory lock every serving instance holds for its whole
// life. It is not a cast artifact (cleanOutDir leaves it alone) and is never
// removed: unlinking a flocked file lets two processes end up locking two
// different inodes under the same name.
const lockName = ".cast.lock"

// pollInterval is how often the takeover paths re-check a lock or a pid.
// Short enough that a normal handover costs milliseconds, long enough to be
// free.
const pollInterval = 50 * time.Millisecond

// DirLock is exclusive ownership of an output directory. The kernel releases
// the lock when the process exits for any reason, kill -9 included, so it is
// the one liveness signal that cannot be fooled by a stale pidfile or an
// unreaped zombie: a live owner always holds it, a dead one never does.
type DirLock struct{ f *os.File }

func (l *DirLock) Release() {
	if l == nil || l.f == nil {
		return
	}
	syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	l.f.Close()
	l.f = nil
}

func lockPath(dir string) string { return dir + string(os.PathSeparator) + lockName }

// tryLockDir takes the directory lock without waiting and records who owns it.
// It fails with EWOULDBLOCK while another instance holds the lock.
func tryLockDir(dir, token string) (*DirLock, error) {
	f, err := os.OpenFile(lockPath(dir), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	recordLockOwner(f, token)
	return &DirLock{f: f}, nil
}

// recordLockOwner writes this process's pid and cast token into the lock file,
// so whoever is waiting for the directory knows whom to ask to stop. Failure is
// ignored: the lock itself already guarantees exclusion, the content only
// makes the handover quicker.
func recordLockOwner(f *os.File, token string) {
	if err := f.Truncate(0); err != nil {
		return
	}
	if _, err := f.Seek(0, 0); err != nil {
		return
	}
	f.WriteString(strconv.Itoa(os.Getpid()) + "\n" + token + "\n")
	f.Sync()
}

// lockOwner is what the lock file says about the instance currently serving:
// the pid to signal and the cast token it was started with. ok is false when
// the file holds no usable owner record.
type lockOwner struct {
	pid   int
	token string
	ok    bool
}

// readLockOwner reads the lock file without taking the lock. The owner rewrites
// it once per cast (it truncates the file under the lock), so a read can catch
// it mid-write; a few retries ride that out before giving up.
func readLockOwner(dir string) (lockOwner, bool) {
	for attempt := 0; attempt < 5; attempt++ {
		b, err := os.ReadFile(lockPath(dir))
		if err != nil {
			return lockOwner{}, false
		}
		lines := strings.Split(string(b), "\n")
		pid, perr := strconv.Atoi(strings.TrimSpace(lines[0]))
		if perr != nil || pid <= 0 {
			time.Sleep(pollInterval)
			continue
		}
		token := ""
		if len(lines) > 1 {
			token = strings.TrimSpace(lines[1])
		}
		return lockOwner{pid: pid, token: token, ok: true}, true
	}
	return lockOwner{}, false
}

// isWouldBlock reports whether flock refused because somebody else holds the
// lock — the expected, retryable case — as opposed to a real filesystem error.
func isWouldBlock(err error) bool {
	return err == syscall.EWOULDBLOCK || err == syscall.EAGAIN || err == syscall.EACCES
}
