package main

import (
	"context"
	"log"
	"net"
	"sort"
	"sync"
	"time"
)

// RelayNode 表示一个中继节点
type RelayNode struct {
	ID          string        // 节点ID
	Address     string        // 节点地址
	IP          string        // 解析后的IP
	Score       float64       // 节点评分
	LastTest    time.Time     // 最后测试时间
	Latency     time.Duration // 延迟
	SuccessRate float64       // 成功率
	Weight      float64       // 权重（用于负载均衡）
}

// RelayNodeManager 管理所有中继节点
type RelayNodeManager struct {
	nodes     []*RelayNode
	mu        sync.RWMutex
	testTimer *time.Ticker
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewRelayNodeManager 创建新的中继节点管理器
func NewRelayNodeManager() *RelayNodeManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RelayNodeManager{
		ctx:    ctx,
		cancel: cancel,
	}
}

// AddNode 添加节点
func (m *RelayNodeManager) AddNode(address string, defaultPort string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		host = address
		port = defaultPort
	}

	ip := net.ParseIP(host)
	if ip != nil {
		var addr string
		if ip.To4() == nil {
			addr = "[" + ip.String() + "]:" + port
		} else {
			addr = ip.String() + ":" + port
		}
		node := &RelayNode{
			ID:      addr,
			Address: addr,
			IP:      addr,
			Score:   50.0,
		}
		m.mu.Lock()
		m.nodes = append(m.nodes, node)
		m.mu.Unlock()
		return nil
	}

	addrs, err := net.LookupIP(host)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ip := range addrs {
		var addr string
		if ip.To4() == nil {
			addr = "[" + ip.String() + "]:" + port
		} else {
			addr = ip.String() + ":" + port
		}
		node := &RelayNode{
			ID:      addr,
			Address: address,
			IP:      addr,
			Score:   50.0,
		}
		m.nodes = append(m.nodes, node)
	}
	return nil
}

// AddNodeAndTest 添加节点并测试速度
func (m *RelayNodeManager) AddNodeAndTest(address string, defaultPort string) ([]string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		host = address
		port = defaultPort
	}

	var addedIPs []string

	ip := net.ParseIP(host)
	if ip != nil {
		var addr string
		if ip.To4() == nil {
			addr = "[" + ip.String() + "]:" + port
		} else {
			addr = ip.String() + ":" + port
		}
		node := &RelayNode{
			ID:      addr,
			Address: addr,
			IP:      addr,
			Score:   50.0,
		}
		if err := m.TestNodeSpeed(node); err != nil {
			log.Printf("[中转节点] TCP连接测试失败: %s, 错误: %v (节点已加入列表，等待后台测速)", addr, err)
			node.Latency = 9999 * time.Second
			node.SuccessRate = 0.0
		} else {
			node.SuccessRate = 1.0
			addedIPs = append(addedIPs, node.IP)
		}
		node.LastTest = time.Now()
		node.Score = node.CalculateScore()

		m.mu.Lock()
		m.nodes = append(m.nodes, node)
		m.mu.Unlock()
		return addedIPs, nil
	}

	addrs, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}

	log.Printf("[中转节点] 域名 %s 解析到 %d 个IP地址", host, len(addrs))

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ip := range addrs {
		var addr string
		if ip.To4() == nil {
			addr = "[" + ip.String() + "]:" + port
		} else {
			addr = ip.String() + ":" + port
		}
		node := &RelayNode{
			ID:      addr,
			Address: addr,
			IP:      addr,
			Score:   50.0,
		}
		if err := m.TestNodeSpeed(node); err != nil {
			log.Printf("[中转节点] TCP连接测试失败: %s, 错误: %v (节点已加入列表，等待后台测速)", addr, err)
			node.Latency = 9999 * time.Second
			node.SuccessRate = 0.0
		} else {
			node.SuccessRate = 1.0
			addedIPs = append(addedIPs, node.IP)
		}
		node.LastTest = time.Now()
		node.Score = node.CalculateScore()
		m.nodes = append(m.nodes, node)
	}
	return addedIPs, nil
}

// TestNodeSpeed 测试节点速度
func (m *RelayNodeManager) TestNodeSpeed(node *RelayNode) error {
	start := time.Now()

	// TCP连接测试
	conn, err := net.DialTimeout("tcp", node.Address, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	latency := time.Since(start)
	node.Latency = latency

	// 可选：发送简单的HTTP HEAD请求测试响应时间
	// httpClient := &http.Client{
	// 	Timeout: 5 * time.Second,
	// 	Transport: &http.Transport{
	// 		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
	// 			return conn, nil
	// 		},
	// 	},
	// }
	//
	// req, _ := http.NewRequest("HEAD", "http://"+node.Address, nil)
	// resp, err := httpClient.Do(req)
	// if err == nil && resp.StatusCode == 200 {
	// 	node.SuccessRate = 1.0
	// } else {
	// 	node.SuccessRate = 0.5
	// }

	return nil
}

// CalculateScore 计算节点评分
func (node *RelayNode) CalculateScore() float64 {
	// 基础评分 = (1 - 归一化延迟) * 0.7 + 成功率 * 0.3
	maxLatency := 5000 * time.Millisecond // 5秒为最大可接受延迟
	normalizedLatency := float64(node.Latency) / float64(maxLatency)
	if normalizedLatency > 1.0 {
		normalizedLatency = 1.0
	}

	baseScore := (1.0-normalizedLatency)*0.7 + node.SuccessRate*0.3

	// 衰减因子：根据最后测试时间衰减评分
	hoursSinceTest := time.Since(node.LastTest).Hours()
	decayFactor := 1.0
	if hoursSinceTest > 1 {
		decayFactor = 1.0 / (1.0 + hoursSinceTest*0.1)
	}

	return baseScore * decayFactor
}

// SelectBestNode 选择最佳节点
func (m *RelayNodeManager) SelectBestNode() *RelayNode {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil
	}

	// 按评分排序
	sortedNodes := make([]*RelayNode, len(m.nodes))
	copy(sortedNodes, m.nodes)
	sort.Slice(sortedNodes, func(i, j int) bool {
		return sortedNodes[i].Score > sortedNodes[j].Score
	})

	return sortedNodes[0]
}

// SelectBestNodes 选择最多n个最佳节点
func (m *RelayNodeManager) SelectBestNodes(n int) []*RelayNode {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil
	}

	// 按评分排序
	sortedNodes := make([]*RelayNode, len(m.nodes))
	copy(sortedNodes, m.nodes)
	sort.Slice(sortedNodes, func(i, j int) bool {
		return sortedNodes[i].Score > sortedNodes[j].Score
	})

	if n > len(sortedNodes) {
		n = len(sortedNodes)
	}

	return sortedNodes[:n]
}

// GetNodeByIP 根据IP获取节点
func (m *RelayNodeManager) GetNodeByIP(ip string) *RelayNode {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		if node.IP == ip {
			return node
		}
	}
	return nil
}

// Start 启动后台测速任务
func (m *RelayNodeManager) Start() {
	m.testTimer = time.NewTicker(30 * time.Second)
	go m.speedTestLoop()
}

// speedTestLoop 测速循环
func (m *RelayNodeManager) speedTestLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.testTimer.C:
			m.testAllNodes()
		}
	}
}

// testAllNodes 测试所有节点速度并更新评分
func (m *RelayNodeManager) testAllNodes() {
	m.mu.RLock()
	nodes := make([]*RelayNode, len(m.nodes))
	copy(nodes, m.nodes)
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *RelayNode) {
			defer wg.Done()
			if err := m.TestNodeSpeed(n); err != nil {
				n.Latency = 9999 * time.Second
				n.SuccessRate = 0.0
			} else {
				n.SuccessRate = 1.0
			}
			n.LastTest = time.Now()
			n.Score = n.CalculateScore()
			n.Weight = n.Score
		}(node)
	}
	wg.Wait()
}

// Stop 停止管理器
func (m *RelayNodeManager) Stop() {
	m.cancel()
	if m.testTimer != nil {
		m.testTimer.Stop()
	}
}
