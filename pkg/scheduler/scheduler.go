package scheduler

import (
	svcerror "food-delivery-saga/pkg/error"
	"sync"
	"time"
)

type DelayQueue[T any] struct {
	MU     sync.Mutex
	Heap   Heap[T]
	ByID   map[string]*Entry[T]
	Out    chan Entry[T]
	WakeUp chan struct{}
	Closed bool
}

func NewQueue[T any](popBuf int) *DelayQueue[T] {
	dq := &DelayQueue[T]{
		ByID:   make(map[string]*Entry[T]),
		Out:    make(chan Entry[T], popBuf),
		WakeUp: make(chan struct{}, 1),
	}
	go dq.Loop()
	return dq
}

func (dq *DelayQueue[T]) Push(item Entry[T]) error {
	dq.MU.Lock()
	defer dq.MU.Unlock()

	if dq.Closed {
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Scheduler.Push"),
			svcerror.WithMsg("delay queue is closed"),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	if old := dq.ByID[item.ID]; old != nil {
		dq.Heap.Remove(old.Index)
		delete(dq.ByID, item.ID)
	}

	dq.Heap.Push(&item)
	dq.ByID[item.ID] = &item

	dq.Notify()
	return nil
}

func (dq *DelayQueue[T]) Remove(id string) bool {
	dq.MU.Lock()
	defer dq.MU.Unlock()

	item := dq.ByID[id]
	if item == nil {
		return false
	}
	dq.Heap.Remove(item.Index)
	delete(dq.ByID, item.ID)

	dq.Notify()
	return true
}

func (dq *DelayQueue[T]) Close() {
	dq.MU.Lock()
	dq.Closed = true
	dq.MU.Unlock()
	dq.Notify()
}

func (dq *DelayQueue[T]) Notify() {
	select {
	case dq.WakeUp <- struct{}{}:
	default:
	}
}

func (dq *DelayQueue[T]) Loop() {
	var timer *time.Timer

	for {
		empty, closed, next := dq.getQueueState()

		if closed && empty {
			close(dq.Out)
			return
		}

		if empty {
			dq.waitForWakeUp()
			continue
		}

		delay := time.Until(next)
		if delay <= 0 {
			dq.popReadyItems()
			continue
		}

		timer = dq.resetTimer(timer, delay)

		dq.waitForTimerOrWakeUp(timer)
	}
}

func (dq *DelayQueue[T]) getQueueState() (empty, closed bool, next time.Time) {
	dq.MU.Lock()
	defer dq.MU.Unlock()
	empty = dq.Heap.Len() == 0
	closed = dq.Closed
	if !empty {
		next = dq.Heap.Peek().ReadyAt
	}
	return
}

func (dq *DelayQueue[T]) waitForWakeUp() {
	<-dq.WakeUp // wait for push/remove/close
}

func (dq *DelayQueue[T]) popReadyItems() {
	now := time.Now()
	for {
		dq.MU.Lock()
		head := dq.Heap.Peek()
		if head == nil || head.ReadyAt.After(now) {
			dq.MU.Unlock()
			break
		}
		item := dq.Heap.Pop()
		delete(dq.ByID, item.ID)
		dq.MU.Unlock()

		dq.Out <- *item
	}
}

func (dq *DelayQueue[T]) resetTimer(timer *time.Timer, delay time.Duration) *time.Timer {
	if timer == nil {
		return time.NewTimer(delay)
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
	return timer
}

func (dq *DelayQueue[T]) waitForTimerOrWakeUp(timer *time.Timer) {
	select {
	case <-timer.C:
	case <-dq.WakeUp:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}
