package ccloud

import "sync"

type SyncResourceHolder[T any] struct {
	resource T
	lock     sync.Mutex
}

func NewSyncResourceHolder[T any](resource T) *SyncResourceHolder[T] {
	return &SyncResourceHolder[T]{
		resource: resource,
		lock:     sync.Mutex{},
	}
}

func (h *SyncResourceHolder[T]) Get() (T, func()) {
	h.lock.Lock()
	return h.resource, h.lock.Unlock
}
