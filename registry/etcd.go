package registry

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdRegistry struct {
	client     *clientv3.Client
	prefix     string
	mu         sync.Mutex
	leases     map[string]clientv3.LeaseID
	ctx        context.Context
	cancelFunc context.CancelFunc
}

var _ Registry = (*EtcdRegistry)(nil)

func NewEtcdRegistry(endpoints []string, prefix string, dialTimeout time.Duration) (*EtcdRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("rpc registry: connect to etcd failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &EtcdRegistry{
		client:     cli,
		prefix:     prefix,
		leases:     make(map[string]clientv3.LeaseID),
		ctx:        ctx,
		cancelFunc: cancel,
	}, nil
}

func (r *EtcdRegistry) Register(ctx context.Context, serviceName string, addr string, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := fmt.Sprintf("%s/%s/%s", r.prefix, serviceName, addr)
	mapKey := fmt.Sprintf("%s@%s", serviceName, addr)

	if _, ok := r.leases[mapKey]; ok {
		r.mu.Unlock()
		_ = r.Deregister(ctx, serviceName, addr)
		r.mu.Lock()
	}

	leaseResp, err := r.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("rpc registry: etcd grant lease failed: %w", err)
	}
	leaseID := leaseResp.ID

	_, err = r.client.Put(ctx, key, addr, clientv3.WithLease(leaseID))
	if err != nil {
		_, _ = r.client.Revoke(ctx, leaseID)
		return fmt.Errorf("rpc registry: etcd put service key failed: %w", err)
	}

	keepAliveChan, err := r.client.KeepAlive(r.ctx, leaseID)
	if err != nil {
		_, _ = r.client.Revoke(ctx, leaseID)
		return fmt.Errorf("rpc registry: etcd keep alive failed: %w", err)
	}

	r.leases[mapKey] = leaseID

	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case resp, ok := <-keepAliveChan:
				if !ok {
					log.Printf("rpc registry: keep alive channel closed for key: %s", key)
					r.mu.Lock()
					delete(r.leases, mapKey)
					r.mu.Unlock()
					return
				}
				if resp == nil {
					log.Printf("rpc registry: keep alive response lost for key: %s", key)
					return
				}
			}
		}
	}()

	return nil
}

func (r *EtcdRegistry) Deregister(ctx context.Context, serviceName string, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	mapKey := fmt.Sprintf("%s@%s", serviceName, addr)
	leaseID, ok := r.leases[mapKey]
	if !ok {
		return nil
	}

	delete(r.leases, mapKey)

	_, err := r.client.Revoke(ctx, leaseID)
	if err != nil {
		return fmt.Errorf("rpc registry: etcd revoke lease failed: %w", err)
	}

	return nil
}

func (r *EtcdRegistry) Close() error {
	r.cancelFunc()

	r.mu.Lock()
	defer r.mu.Unlock()

	for mapKey, leaseID := range r.leases {
		_, _ = r.client.Revoke(context.Background(), leaseID)
		delete(r.leases, mapKey)
	}

	return r.client.Close()
}
