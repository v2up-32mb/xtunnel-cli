# x-tunnel 代码拆分说明

## 文件结构

```
x-tunnel/
├── client/                    # 客户端专用代码
│   ├── config.go             # 客户端配置
│   ├── pool.go               # 连接池管理
│   ├── socks5.go             # SOCKS5 代理实现
│   ├── udp.go                # UDP 关联实现
│   └── websocket.go          # WebSocket 连接管理
├── common.go                 # 公共代码（错误处理等）
├── errors.go                 # 错误定义
├── ip_strategy.go            # IP 策略解析
├── protocol.go               # 协议定义
└── x-tunnel-client.go        # 客户端入口
```

## 拆分说明

### client/config.go
- `GlobalConfig` 结构体和全局配置实例
- 包含超时、缓冲区大小等配置参数

### client/pool.go
- `ClientPool` 连接池实现
- `ClientConnState` 连接状态管理
- `WriteJob` 写入任务定义
- 多通道管理、消息处理、写入聚合等核心逻辑

### client/socks5.go
- `ProxyConfig` SOCKS5 代理配置
- `parseAuthAndAddr` 解析认证和地址
- `runSOCKS5Listener` 启动 SOCKS5 监听器
- `handleSOCKS5` 处理 SOCKS5 连接
- `handleSOCKS5UserPassAuth` 用户名密码认证
- `handleSOCKS5Connect` 处理 CONNECT 请求
- `handleSOCKS5UDP` 处理 UDP ASSOCIATE 请求

### client/udp.go
- `UDPAssociation` UDP 关联管理
- `parseSOCKS5UDPPacket` 解析 SOCKS5 UDP 数据包
- `buildSOCKS5UDPPacket` 构建 SOCKS5 UDP 数据包
- UDP 数据收发逻辑

### client/websocket.go
- `buildTLSConfig` 构建 TLS 配置
- `dialWebSocket` 建立 WebSocket 连接

### common.go
- `shortID` 短 ID 生成
- `isNormalCloseError` 判断正常关闭错误
- `containsString` 字符串包含检查
- `findSubstring` 查找子串

### errors.go
- `ErrOnlyWSS` 仅支持 WSS 协议错误
- `ErrAuthFailed` 认证失败错误

### ip_strategy.go
- IP 策略常量定义
- `parseIPStrategy` 解析 IP 策略参数

### protocol.go
- 消息类型定义
- `ConnStatus` 连接状态枚举
- `encodeMessage` 编码消息
- `decodeMessage` 解码消息

### x-tunnel-client.go
- 客户端入口函数 `main`
- 命令行参数定义
- 信号处理和优雅关闭
- 启动 SOCKS5 监听器和连接池

## 优化点

1. **模块化拆分**：按功能拆分为多个文件，每个文件职责单一
2. **代码可读性**：添加了详细的中文注释
3. **可维护性**：相关功能集中管理，便于修改和扩展
4. **可扩展性**：清晰的模块划分便于添加新功能
5. **编译正常**：所有拆分后的代码编译通过

## 编译命令

```bash
# 客户端
go build -tags client -o x-tunnel-client .

# 服务端（如果有服务端代码）
go build -tags server -o x-tunnel-server .
```
