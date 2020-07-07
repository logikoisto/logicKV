package raftkv

import "sync"

type PubSub struct {
	mu   sync.RWMutex
	subs map[int][]chan Op
}

func NewPubSub() *PubSub {
	ps := &PubSub{}
	ps.subs = make(map[int][]chan Op)
	return ps
}

func (ps *PubSub) Publish(topic int, op Op) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, ch := range ps.subs[topic] {
		go func(ch chan Op) {
			ch <- op
		}(ch)
	}
}

func (ps *PubSub) Subscribe(topic int) <-chan Op {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ch := make(chan Op, 1)
	ps.subs[topic] = append(ps.subs[topic], ch)
	return ch
}
