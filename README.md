# dRPC (Distributed RPC Framework)

![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.18-blue)

dRPC 是一个基于 Go 语言从零实现的轻量级分布式 RPC 框架，提供了类似于 gRPC / Dubbo 的基础微服务调用骨架。

## 核心特性

* **服务注册与发现**：原生集成 etcd。服务端支持 Lease 心跳续约；客户端基于 Watch 机制动态更新本地路由缓存。
* **负载均衡**：解耦 `Discovery` 与 `Balancer`，支持按服务名隔离路由，内置多种算法。
* **中间件机制**：基于洋葱模型实现链式中间件，可无侵入式挂载 Panic Recovery、日志、限流等功能。
* **多协议与降级**：内置 Gob、JSON 编解码支持，并支持 HTTP 隧道降级。

---

## 快速开始

### 1. 安装与准备
确保本地已安装 Go (>=1.18) 并运行 etcd (默认端口 2379)。
```bash
go get github.com/WCBBEX/drpc
```

### 2. 定义服务
```go
type Calculator struct{}

// 方法签名必须符合规范：入参两个，返回值 error
func (c *Calculator) Add(args *Args, reply *int) error {
    *reply = args.A + args.B
    return nil
}
```

### 3. Server 端：注册服务 & 挂载中间件
```go
func main() {
    // 1. 接入 etcd 注册中心
    reg, _ := registry.NewEtcdRegistry([]string{"127.0.0.1:2379"}, "/drpc", time.Second*5)
    defer reg.Close()

    // 2. 初始化 Server
    server := drpc.NewServer()
    // 3. 注册本地服务
    server.Register(&Calculator{})
    
    // 4. 发布服务到 etcd (TTL 10秒)
    addr := "127.0.0.1:9999"
    reg.Register(context.Background(), "Calculator", addr, 10*time.Second)

    // 5. 启动 TCP 监听
    l, _ := net.Listen("tcp", addr)
    server.Accept(l)
}
```

### 4. Client 端：服务发现 & 分布式调用
```go
func main() {
    // 1. 初始化 etcd 服务发现 (Watch 动态缓存)
    d, _ := registry.NewEtcdDiscovery([]string{"127.0.0.1:2379"}, "/drpc", time.Second*5)
    defer d.Close()

    // 2. 配置负载均衡器 (轮询)
    balancer := xclient.NewRoundRobinBalancer()

    // 3. 构建分布式客户端
    xc := xclient.NewXClient(d, balancer, drpc.DefaultOption)
    defer xc.Close()

    // 4. 发起 RPC 调用 (dRPC 会自动拉取节点并进行负载均衡调度)
    var reply int
    err := xc.Call(context.Background(), "Calculator.Add", &Args{A: 10, B: 20}, &reply)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Result: %d\n", reply)
}
```