package xclient

import (
	"errors"
	"io"
	"sync"
)

type Discovery interface {
	io.Closer
	GetServices(serviceName string) ([]string, error)
	Refresh(serviceName string) error
}

type BasicServersDiscovery struct {
	mu       sync.RWMutex
	services map[string][]string
}

func NewMultiServerDiscovery() *BasicServersDiscovery {
	return &BasicServersDiscovery{}
}

var _ Discovery = (*BasicServersDiscovery)(nil)

func (d *BasicServersDiscovery) Refresh(serviceName string) error {
	return nil
}

func (d *BasicServersDiscovery) GetServices(serviceName string) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	servers, ok := d.services[serviceName]
	if !ok || len(servers) == 0 {
		return nil, errors.New("rpc discovery: no available servers for service: " + serviceName)
	}

	serversCopy := make([]string, len(servers))
	copy(serversCopy, servers)
	return serversCopy, nil
}

func (d *BasicServersDiscovery) Close() error {
	return nil
}

func (d *BasicServersDiscovery) Update(serviceName string, servers []string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.services[serviceName] = servers
}
