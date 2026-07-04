package discovery

import (
	"context"
	"fmt"
	. "github.com/WCBBEX/drpc/utils"
	"log"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdDiscovery struct {
	client     *clientv3.Client
	prefix     string
	mu         sync.RWMutex
	services   map[string][]string
	watching   map[string]context.CancelFunc
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewEtcdDiscovery(endpoints []string, prefix string, dialTimeout time.Duration) (*EtcdDiscovery, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("rpc discovery: connect to etcd failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &EtcdDiscovery{
		client:     cli,
		prefix:     prefix,
		services:   make(map[string][]string),
		watching:   make(map[string]context.CancelFunc),
		ctx:        ctx,
		cancelFunc: cancel,
	}, nil
}

func (d *EtcdDiscovery) GetServices(serviceName string) ([]string, error) {
	d.mu.RLock()

	if _, ok := d.watching[serviceName]; ok {
		servers := d.services[serviceName]
		d.mu.RUnlock()
		return CopySlice(servers), nil
	}
	d.mu.RUnlock()

	if err := d.initService(serviceName); err != nil {
		return nil, err
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	return CopySlice(d.services[serviceName]), nil
}

func (d *EtcdDiscovery) Refresh(serviceName string) error {
	servicePrefix := fmt.Sprintf("%s/%s", d.prefix, serviceName)
	ctx, cancel := context.WithTimeout(d.ctx, 3*time.Second)
	resp, err := d.client.Get(ctx, servicePrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return fmt.Errorf("rpc discovery: refresh failed for %s: %w", serviceName, err)
	}

	var servers []string
	for _, kv := range resp.Kvs {
		addr := strings.TrimPrefix(string(kv.Key), servicePrefix+"/")
		if addr != "" {
			servers = append(servers, addr)
		}
	}

	d.mu.Lock()
	d.services[serviceName] = servers
	d.mu.Unlock()
	return nil
}

func (d *EtcdDiscovery) Close() error {
	d.cancelFunc()

	d.mu.Lock()
	defer d.mu.Unlock()

	for _, cancel := range d.watching {
		cancel()
	}

	return d.client.Close()
}

func (d *EtcdDiscovery) initService(serviceName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.watching[serviceName]; ok {
		return nil
	}

	servicePrefix := fmt.Sprintf("%s/%s", d.prefix, serviceName)

	ctx, cancel := context.WithTimeout(d.ctx, 3*time.Second)
	resp, err := d.client.Get(ctx, servicePrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return fmt.Errorf("rpc discovery: init get failed for %s: %w", serviceName, err)
	}

	var servers []string
	for _, kv := range resp.Kvs {
		addr := strings.TrimPrefix(string(kv.Key), servicePrefix+"/")
		if addr != "" {
			servers = append(servers, addr)
		}
	}
	d.services[serviceName] = servers

	watchCtx, watchCancel := context.WithCancel(d.ctx)
	d.watching[serviceName] = watchCancel

	go d.watchWorker(watchCtx, serviceName, servicePrefix)

	return nil
}

func (d *EtcdDiscovery) watchWorker(ctx context.Context, serviceName string, servicePrefix string) {
	watchChan := d.client.Watch(ctx, servicePrefix, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watchChan:
			if !ok {
				log.Printf("rpc discovery: watch channel closed for service: %s", serviceName)
				return
			}
			if resp.Err() != nil {
				log.Printf("rpc discovery: watch response error for %s: %v", serviceName, resp.Err())
				return
			}

			for _, ev := range resp.Events {
				addr := strings.TrimPrefix(string(ev.Kv.Key), servicePrefix+"/")
				if addr == "" {
					continue
				}

				switch ev.Type {
				case mvccpb.PUT:
					d.addServer(serviceName, addr)
				case mvccpb.DELETE:
					d.removeServer(serviceName, addr)
				}
			}
		}
	}
}

func (d *EtcdDiscovery) addServer(serviceName string, addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	servers := d.services[serviceName]
	for _, s := range servers {
		if s == addr {
			return
		}
	}
	d.services[serviceName] = append(servers, addr)
	log.Printf("rpc discovery: service [%s] dynamically added node: %s", serviceName, addr)
}

func (d *EtcdDiscovery) removeServer(serviceName string, addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	servers := d.services[serviceName]
	for i, s := range servers {
		if s == addr {
			d.services[serviceName] = append(servers[:i], servers[i+1:]...)
			log.Printf("rpc discovery: service [%s] dynamically removed node: %s", serviceName, addr)
			return
		}
	}
}
