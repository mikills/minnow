//go:build linux

package memlimit

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These run only where CI has put this process in a cgroup v2 it can write, so
// the parts that talk to the kernel are exercised somewhere. Everything else in
// this package is path-based and runs anywhere.
func requireOwnCgroup(t *testing.T) string {
	t.Helper()
	if os.Getenv("MINNOW_CGROUP_IT") == "" {
		t.Skip("set MINNOW_CGROUP_IT=1 inside a writable cgroup v2 to run")
	}
	dir, err := ownCgroupDir()
	require.NoError(t, err)
	return dir
}

func readValue(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return strings.TrimSpace(string(raw))
}

func pressureTotal(t *testing.T, path string) uint64 {
	t.Helper()
	for _, line := range strings.Split(readValue(t, path), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "total="); ok {
				total, err := strconv.ParseUint(value, 10, 64)
				require.NoError(t, err)
				return total
			}
		}
	}
	t.Fatalf("no cumulative some pressure in %s", path)
	return 0
}

func requirePrivatePressure(t *testing.T, watcher *Watcher) {
	t.Helper()
	// Deadlines require runtime poll registration. Our epoll must be the only
	// poller allowed to consume this descriptor's PSI notification latch.
	require.ErrorIs(t, watcher.file.SetReadDeadline(time.Time{}), os.ErrNoDeadline)
}

func TestCgroupIntegration(t *testing.T) {
	dir := requireOwnCgroup(t)

	t.Run("global pressure can be armed without a runtime poller", func(t *testing.T) {
		watcher, err := watchPressure(globalPressurePath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, watcher.Close()) })
		requirePrivatePressure(t, watcher)
		require.NoError(t, watcher.Close())
	})

	t.Run("the ceiling is the cgroup limit, not the host", func(t *testing.T) {
		limit := Detect()
		require.NoError(t, limit.Usable())
		require.Equal(t, "cgroup", limit.Source)

		want := readValue(t, filepath.Join(dir, "memory.max"))
		expected, err := strconv.ParseInt(want, 10, 64)
		require.NoError(t, err, "the test cgroup needs a numeric memory.max")
		require.Equal(t, expected, limit.Ceiling)
		require.Equal(t, dir, limit.Dir())
	})

	t.Run("usage is a working set, below the limit", func(t *testing.T) {
		usage := Current(dir)
		require.True(t, usage.Ok)
		require.Equal(t, "working set", usage.Source)
		require.Positive(t, usage.Bytes)
		require.Less(t, usage.Bytes, Detect().Ceiling)
	})

	t.Run("memory.high is written at the backstop and restored", func(t *testing.T) {
		before := readValue(t, filepath.Join(dir, "memory.high"))
		ceiling := Detect().Ceiling

		watcher, err := Watch(dir, ceiling)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, watcher.Close()) })
		require.True(t, watcher.Enforced(), "the cgroup was writable but nothing was enforced")
		// Stall time, not the events counter: memory.high is policed against
		// memory.current, so page cache at the mark wakes an events watcher
		// continuously while nothing actionable has changed.
		require.Equal(t, "memory.pressure", watcher.Source())

		// The kernel stores memory.high rounded down to a page multiple, so the
		// value asked for has to be aligned or the mark is not where we put it.
		during, err := strconv.ParseInt(readValue(t, filepath.Join(dir, "memory.high")), 10, 64)
		require.NoError(t, err)
		want := int64(float64(ceiling) * BackstopMark)
		want -= want % int64(os.Getpagesize())
		require.Equal(t, want, during, "the kernel rounded the mark we asked for")

		require.NoError(t, watcher.Close())
		// Left written, the next start reads it back as the ceiling and takes
		// 95% of that, walking the budget down on every restart.
		require.Equal(t, before, readValue(t, filepath.Join(dir, "memory.high")))
	})

	t.Run("crossing the mark wakes the watcher, and quiet blocks it", func(t *testing.T) {
		ceiling := Detect().Ceiling
		// A brief excursion above 95% of memory.max can produce less than the
		// trigger's 150ms of stall (63ms on one runner), even when every page is
		// touched. Use a lower test-only budget and a larger relative overage,
		// with ample headroom below the real hard limit. The production trigger
		// and Watch's backstop calculation remain under test.
		const testBudget int64 = 256 << 20
		require.GreaterOrEqual(t, ceiling, 4*testBudget, "the test needs hard-limit headroom")
		require.Equal(t, "0", readValue(t, filepath.Join(dir, "memory.swap.max")),
			"the bounded anonymous pressure fixture requires swap disabled in its test cgroup")
		watcher, err := Watch(dir, testBudget)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, watcher.Close()) })
		require.Equal(t, "memory.pressure", watcher.Source())
		require.True(t, watcher.Enforced())
		requirePrivatePressure(t, watcher.(*Watcher))
		pressure := filepath.Join(dir, "memory.pressure")
		before := pressureTotal(t, pressure)
		t.Logf("watcher source=%s enforced=%t memory.high=%s memory.max=%d some total=%d",
			watcher.Source(), watcher.Enforced(), readValue(t, filepath.Join(dir, "memory.high")), ceiling, before)

		hog := make([][]byte, 0, 384) // At most 384MiB against ~243MiB memory.high.
		t.Cleanup(func() {
			hog = nil
			debug.FreeOSMemory()
		})
		// Wait treats a timeout as success for the governor's periodic sampling.
		// Start the infinite waiter and watchdog before allocating: memory.high
		// can throttle each charge for seconds. Stop adding pressure as soon as
		// the notification arrives, and bound overshoot to one small allocation.
		done := make(chan struct{})
		var waitErr error
		var stopped bool
		start := time.Now()
		expired := make(chan struct{})
		watchdog := time.AfterFunc(20*time.Second, func() {
			watcher.Interrupt()
			close(expired)
		})
		go func() {
			waitErr = watcher.Wait(-1)
			// Cancel at notification time, not after the allocator finishes its
			// current throttled chunk. A late Interrupt would poison quiet checks.
			stopped = watchdog.Stop()
			if !stopped {
				<-expired
			}
			close(done)
		}()
		t.Cleanup(func() {
			watcher.Interrupt()
			<-done // Joins the waiter and watchdog before Close.
		})
	pressure:
		for range cap(hog) {
			select {
			case <-done:
				break pressure
			default:
			}
			block := make([]byte, 1<<20)
			for i := range block {
				block[i] = 1
			}
			hog = append(hog, block)
		}
		<-done
		elapsed := time.Since(start)
		runtime.KeepAlive(hog)
		after := pressureTotal(t, pressure)
		t.Logf("pressure stimulus/wait=%s allocated=%dMiB err=%v some total=%d -> %d memory.current=%s memory.swap.current=%s",
			elapsed, len(hog), waitErr, before, after, readValue(t, filepath.Join(dir, "memory.current")),
			readValue(t, filepath.Join(dir, "memory.swap.current")))
		require.True(t, stopped, "pressure notification did not arrive before the watchdog")
		require.NoError(t, waitErr, "reclaiming past memory.high did not wake the watcher")
		// The exported total uses PSI_AVGS; privileged triggers use PSI_POLL.
		// Their independently sampled, CPU-weighted totals need not agree at
		// the threshold. The infinite wait above proves a notification arrived;
		// this counter only corroborates that the stimulus produced stalls.
		require.Greater(t, after, before, "the allocation did not produce memory stalls")

		hog = nil
		debug.FreeOSMemory()
		require.Eventually(t, func() bool {
			usage := Current(dir)
			return usage.Ok && usage.Bytes < testBudget/2
		}, 20*time.Second, 200*time.Millisecond, "memory was never returned")

		// Low usage alone does not establish quiet: reclaim can still be stalling
		// and PSI can retain a rate-limited notification from its tracking window.
		// Keep the same watcher; require two full one-second windows with no new
		// stall time, then consume at most one already-latched notification.
		total := pressureTotal(t, pressure)
		stableSince := time.Now()
		deadline := stableSince.Add(20 * time.Second)
		for time.Now().Before(deadline) && time.Since(stableSince) < 2*time.Second {
			time.Sleep(200 * time.Millisecond)
			current := pressureTotal(t, pressure)
			if current != total {
				t.Logf("settling some total=%d -> %d", total, current)
				total, stableSince = current, time.Now()
			}
		}
		require.GreaterOrEqual(t, time.Since(stableSince), 2*time.Second,
			"PSI did not stabilize within 20s; some total=%d", total)
		require.NoError(t, watcher.Wait(0), "draining the pending pressure notification")
		for i := range 3 {
			start := time.Now()
			err := watcher.Wait(1000)
			elapsed := time.Since(start)
			current := pressureTotal(t, pressure)
			t.Logf("quiet wait %d=%s err=%v some total=%d -> %d", i+1, elapsed, err, total, current)
			require.NoError(t, err)
			// Incidental subthreshold stalls do not warrant a notification.
			require.GreaterOrEqual(t, elapsed, 900*time.Millisecond,
				"the watcher woke with no stall to report")
			total = current
		}
	})

	t.Run("an Interrupt after Close does not write to a released descriptor", func(t *testing.T) {
		watcher, err := Watch(dir, Detect().Ceiling)
		require.NoError(t, err)
		require.NoError(t, watcher.Close())
		watcher.Interrupt()
		watcher.Interrupt()
	})

	t.Run("the real cgroup root has no memory.max and is not confinement", func(t *testing.T) {
		// Against the kernel's own filesystem, not a fixture. A process in the
		// v2 root used to read as confined, which cost it sizing and the
		// governor both.
		self := filepath.Join(t.TempDir(), "cgroup")
		require.NoError(t, os.WriteFile(self, []byte("0::/\n"), 0o644))

		_, err := os.Stat(filepath.Join(cgroupRoot, "memory.max"))
		require.ErrorIs(t, err, os.ErrNotExist, "this test is meaningless if the root has a limit")

		limit, _, confined := cgroupLimit(cgroupRoot, self)
		require.Zero(t, limit)
		require.False(t, confined)
	})
}
