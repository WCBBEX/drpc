package discovery

import (
	"errors"
	"io"
	"sync"

	"github.com/WCBBEX/drpc/utils"
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

func NewBasicServerDiscovery(services map[string][]string) *BasicServersDiscovery {
	return &BasicServersDiscovery{services: services}
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

	return utils.CopySlice(servers), nil
}

func (d *BasicServersDiscovery) Close() error {
	return nil
}

func (d *BasicServersDiscovery) Update(serviceName string, servers []string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.services[serviceName] = servers
}
