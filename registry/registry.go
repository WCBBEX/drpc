package registry

import (
	"context"
	"time"
)

type Registry interface {
	Register(ctx context.Context, serviceName string, addr string, ttl time.Duration) error
	Deregister(ctx context.Context, serviceName string, addr string) error
	Close() error
}
