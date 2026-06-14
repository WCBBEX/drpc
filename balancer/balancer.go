package balancer

import "errors"

var ErrNoAvailableServers = errors.New("rpc balancer: no available servers")

type Balancer interface {
	Choose(servers []string) (string, error)
}
