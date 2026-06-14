package balancer

import "sync"

type RoundRobinBalancer struct {
	mu    sync.Mutex
	index int
}

func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{index: 0}
}

func (b *RoundRobinBalancer) Choose(servers []string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(servers)
	if n == 0 {
		return "", ErrNoAvailableServers
	}

	addr := servers[b.index%n]
	b.index = (b.index + 1) % n
	return addr, nil
}
