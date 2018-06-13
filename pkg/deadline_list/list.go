package deadline_list

import (
	"log"
	"math"
	"time"
)

type item struct {
	deadline time.Time
	value    interface{}
	prev     *item
	next     *item
	id       int64
}

// List is a deadline list.
type List struct {
	pending  map[int64]*item
	nextItem int64
	lastItem int64
	timer    *time.Timer
	ttl      time.Duration
}

// New creates a new deadline list.
func New(ttl time.Duration) *List {
	al := &List{
		pending:  nil,
		nextItem: -1,
		lastItem: -1,
		timer:    time.NewTimer(time.Duration(math.MaxInt64)),
		ttl:      ttl,
	}

	return al
}

// Wait returns a chan to alert on new elements passed their deadline.
func (al *List) Wait() <-chan time.Time {
	if al == nil {
		return nil
	}

	return al.timer.C
}

// Close can be called to remove pending items.
// Returns all the pending items.
func (al *List) Close() []interface{} {
	if al == nil {
		return nil
	}

	var res []interface{}
	for _, v := range al.pending {
		res = append(res, v.value)
	}

	al.timer.Stop()
	al.nextItem = -1
	al.pending = nil

	return res
}

// Pop should be called after wait chan posts
// Returns an item that's passed it's deadline or nil.
func (al *List) Pop() interface{} {
	if al == nil {
		return nil
	}

	if al.nextItem == -1 || al.pending == nil {
		return nil
	}

	item, ok := al.pending[al.nextItem]
	if ok == false {
		log.Fatal("item missing from pending list")
	}

	if item.deadline.After(time.Now()) {
		al.timer.Reset(item.deadline.Sub(time.Now()))
		return nil
	}

	delete(al.pending, al.nextItem)

	if item.next != nil {
		item.next.prev = nil
		al.nextItem = item.next.id
		al.timer.Reset(item.next.deadline.Sub(time.Now()))
	} else {
		al.nextItem = -1
	}

	if item.prev != nil {
		log.Fatal("item should not have prev")
	}

	return item.value
}

// Push adds a new item with a deadline.
// Returns the id of the pushed item.
func (al *List) Push(value interface{}) int64 {
	if al.pending == nil {
		al.pending = make(map[int64]*item)
	}

	prev, _ := al.pending[al.lastItem]
	id := al.lastItem + 1
	al.lastItem = id

	if al.nextItem == -1 {
		al.nextItem = id
		al.timer.Reset(al.ttl)
	}

	item := &item{
		deadline: time.Now().Add(al.ttl),
		value:    value,
		prev:     prev,
		next:     nil,
		id:       id,
	}

	if prev != nil {
		prev.next = item
	}

	al.pending[id] = item
	return id
}

// Remove returns and removes the item from the list.
// Will return nil if the item does not exist or was nil.
func (al *List) Remove(id int64) interface{} {
	if al == nil {
		return nil
	}

	if id < al.nextItem ||
		id > al.lastItem ||
		al.pending == nil {
		return nil
	}

	item, ok := al.pending[id]
	if ok == false {
		return nil
	}
	delete(al.pending, id)

	if item.prev != nil {
		item.prev.next = item.next
	}
	if item.next != nil {
		item.next.prev = item.prev
	}

	if id == al.nextItem {
		if item.next != nil {
			al.nextItem = item.next.id
			al.timer.Reset(item.next.deadline.Sub(time.Now()))
		} else {
			al.nextItem = -1
		}
	}

	return item.value
}
