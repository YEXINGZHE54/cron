package cron

import (
	"container/heap"
	"testing"
	"time"
)

func TestMinHeap(t *testing.T) {
	pq := make(byTime, 0)
	heap.Init(&pq)
	e1 := &Entry{Next: time.Now().Add(2 * ONE_SECOND), Key: "k1"}
	e2 := &Entry{Next: time.Now().Add(5 * ONE_SECOND), Key: "k2"}
	e3 := &Entry{Next: time.Now().Add(1 * ONE_SECOND), Key: "k3"}
	heap.Push(&pq, e1)
	heap.Push(&pq, e2)
	heap.Push(&pq, e3)
	expected := []string{"k3", "k1", "k2"}
	var collectd []string
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Entry)
		collectd = append(collectd, item.Key)
	}
	for i, expr := range expected {
		if expr != collectd[i] {
			t.FailNow()
		}
	}
}
