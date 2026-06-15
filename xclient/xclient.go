package xclient

import (
	"context"
	. "drpc"
	. "drpc/balancer"
	"drpc/discovery"
	"fmt"
	"io"
	"strings"
	"sync"
)

type XClient struct {
	d        discovery.Discovery
	balancer Balancer
	opt      *Option
	mu       sync.Mutex
	clients  map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d discovery.Discovery, b Balancer, opt *Option) *XClient {
	return &XClient{d: d, balancer: b, opt: opt, clients: make(map[string]*Client)}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}

	return nil
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	client, ok := xc.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}

	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}

		xc.clients[rpcAddr] = client
	}

	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply any) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}

	return client.Call(ctx, serviceMethod, args, reply)
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply any) error {
	parts := strings.Split(serviceMethod, ".")
	if len(parts) < 2 {
		return fmt.Errorf("rpc client: invalid serviceMethod format: %s, expect Service.Method", serviceMethod)
	}
	serviceName := parts[0]

	servers, err := xc.d.GetServices(serviceName)
	if err != nil {
		return err
	}

	rpcAddr, err := xc.balancer.Choose(servers)
	if err != nil {
		return err
	}

	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}
