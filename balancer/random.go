package balancer

import (
	"math/rand"
	"time"
)

type RandomBalancer struct {
	r *rand.Rand
}

func NewRandomBalancer() *RandomBalancer {
	return &RandomBalancer{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (b *RandomBalancer) Choose(servers []string) (string, error) {
	n := len(servers)
	if n == 0 {
		return "", ErrNoAvailableServers
	}

	return servers[b.r.Intn(n)], nil
}
