// This library implements a cron spec parser and runner.  See the README for
// more details.
package cron

import (
	"container/heap"
	//"fmt"
	"time"
)

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job
	Pos int
	Key string //stored in map, help to quickly find the entry
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  byTime
	index    map[string]*Entry //for quickly find entry and update it
	stop     chan struct{}
	add      chan *Entry
	remove   chan *Entry
	snapshot chan byTime
	running  bool
}

func (s byTime) Len() int { return len(s) }

func (s byTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
	s[i].Pos = i
	s[j].Pos = j
}
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return true
	}
	if s[j].Next.IsZero() {
		return false
	}
	return s[i].Next.Unix() < s[j].Next.Unix()
}
func (pq *byTime) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Entry)
	item.Pos = n
	*pq = append(*pq, item)
}

func (pq *byTime) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Pos = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (s byTime) Top() (interface{}, bool) {
	if s.Len() > 0 {
		return s[0], true
	}
	return nil, false
}

// update modifies the priority and value of an Item in the queue.
//func (pq *PriorityQueue) update(item *Item, value string, priority int) {
//	item.value = value
//	item.priority = priority
//	heap.Fix(pq, item.index)
//}

// New returns a new Cron job runner.
func New() *Cron {
	r := &Cron{
		entries:  make(byTime, 0),
		index:    make(map[string]*Entry),
		add:      make(chan *Entry),
		stop:     make(chan struct{}),
		remove:   make(chan *Entry),
		snapshot: make(chan byTime),
		running:  false,
	}
	heap.Init(&r.entries)
	return r
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func(), key string) error {
	return c.AddJob(spec, FuncJob(cmd), key)
}

// AddFunc adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job, key string) error {
	schedule, err := Parse(spec)
	if err != nil {
		return err
	}
	c.Schedule(schedule, cmd, key)
	return nil
}

func (c *Cron) Remove(key string) error {
	entry, found := c.index[key]
	if !found {
		return nil
	}
	if !c.running {
		pos := entry.Pos
		heap.Remove(&c.entries, pos)
		c.index[entry.Key] = nil
		return nil
	}
	c.remove <- entry
	return nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, key string) {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		Key:      key}
	if !c.running {
		heap.Push(&c.entries, entry)
		c.index[entry.Key] = entry
		return
	}

	c.add <- entry
	return
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.running = true
	go c.run()
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	var now time.Time

	for {
		var effective time.Time
		var first_entry *Entry
		// 'now' should be updated every loop starts
		now = time.Now().Local()
		for {
			v, ok := c.entries.Top()
			if !ok {
				effective = now.AddDate(10, 0, 0)
				break
			}
			first_entry = v.(*Entry)
			//fmt.Printf("%v", first_entry)
			if first_entry.Next.IsZero() {
				first_entry.Next = first_entry.Schedule.Next(now)
				heap.Fix(&c.entries, first_entry.Pos)
				continue
			} else {
				effective = first_entry.Next
			}
			//fmt.Printf("effective: ")
			//fmt.Println(effective)
			//for _, v := range c.entries {
			//	fmt.Printf("cur entries: %v", v)
			//}
			// effective must not be zero
			if effective.Unix() <= now.Unix() {
				go first_entry.Job.Run()
				first_entry.Prev = first_entry.Next

				first_entry.Next = first_entry.Schedule.Next(effective)
				heap.Fix(&c.entries, first_entry.Pos)
			} else {
				break
			}
		}

		// when came here, effective must be larger than now
		select {
		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for {
				v, ok := c.entries.Top()
				if !ok {
					effective = now.AddDate(10, 0, 0)
					break
				}
				first_entry = v.(*Entry)
				if first_entry.Next.IsZero() {
					first_entry.Next = first_entry.Schedule.Next(now)
					heap.Fix(&c.entries, first_entry.Pos)
					continue
				} else {
					effective = first_entry.Next
				}
				if effective.Unix() <= now.Unix() {
					go first_entry.Job.Run()
					first_entry.Prev = first_entry.Next

					first_entry.Next = first_entry.Schedule.Next(effective)
					heap.Fix(&c.entries, first_entry.Pos)
				} else {
					break
				}
			}

		case newEntry := <-c.add:
			// add into heap
			heap.Push(&c.entries, newEntry)
			c.index[newEntry.Key] = newEntry
			// schedule it right now
			newEntry.Next = newEntry.Schedule.Next(now)

		case entry := <-c.remove:
			pos := entry.Pos
			heap.Remove(&c.entries, pos)
			c.index[entry.Key] = nil

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			return
		}
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() byTime {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}
