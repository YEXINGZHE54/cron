package cron

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const ONE_SECOND = 1*time.Second + 10*time.Millisecond

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron := New()
	cron.Start()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k1")

	select {
	case <-time.After(ONE_SECOND):
		// No job ran!
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k1")
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k1")

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("@every 2s", func() { wg.Done() }, "k1")
	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	select {
	case <-time.After(ONE_SECOND):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "k1")
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k2")
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "k3")
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k4")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	fmt.Println("job twice")
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "k1")
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "k2")
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k3")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	fmt.Println("multiple schedule")
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "k1")
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "k2")
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "k3")
	cron.Schedule(Every(time.Minute), FuncJob(func() {}), "k4")
	cron.Schedule(Every(time.Second), FuncJob(func() { wg.Done() }), "k5")
	cron.Schedule(Every(time.Hour), FuncJob(func() {}), "k6")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	fmt.Println("test timezone")
	wg := &sync.WaitGroup{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+1, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New()
	cron.AddFunc(spec, func() { wg.Done() }, "k1")
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run() {
	t.wg.Done()
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	fmt.Println("test Job")
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddJob("0 0 0 27 Feb ?", testJob{wg, "job0"}, "j1")
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"}, "j2")
	cron.AddJob("* * * * * ?", testJob{wg, "job2"}, "j3")
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"}, "j4")
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"}, "j5")
	cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"}, "j6")

	cron.Start()
	defer cron.Stop()
	fmt.Println("before select")
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Add a job, start cron, expect it not runs.
func TestRemoveBeforeRunning(t *testing.T) {
	fmt.Println("remove 1")
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "key1")
	cron.Remove("key1")
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):

	case <-wait(wg):
		t.FailNow()
	}
}

// Start cron, add a job, expect it runs.
func TestRemoveWhileRunning(t *testing.T) {
	fmt.Println("remove 2")
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "key1")
	cron.Start()
	defer cron.Stop()

	// after one second, remove job
	select {
	case <-time.After(ONE_SECOND):
		cron.Remove("key1")
	}

	select {
	case <-time.After(ONE_SECOND):

	case <-wait(wg):
		t.FailNow()
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		select {
		case ch <- true:
		default:
		}
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		select {
		case ch <- true:
		default:
		}
	}()
	return ch
}
