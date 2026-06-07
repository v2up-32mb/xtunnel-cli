package client

import (
	"context"
	"log"
	"net"
	"sort"
	"sync"
	"time"
)

// RelayNode 表示一个中转节点
type RelayNode struct {
	ID          string        // 节点ID
	Address     string        // 节点源地址（IP:PORT 或 hostname:PORT）
	IP          string        // 当前用于连接的解析后地址
	Score       float64       // 节点评分
	LastTest    time.Time     // 最后测试时间
	Latency     time.Duration // 延迟
	SuccessRate float64       // 成功率
	Weight      float64       // 权重（用于负载均衡）
	FailCount   int           // 连续失败次数
	FailTime    time.Time     // 最近失败时间
	mu          sync.RWMutex  // 保护字段的并发访问
}

// RelayNodeManager 管理所有中转节点
type RelayNodeManager struct {
	nodes     []*RelayNode
	mu        sync.RWMutex
	testTimer *time.Ticker
	ctx       context.Context
	cancel    context.CancelFunc
	lookupIP  func(host string) ([]net.IP, error)
}

type relayNodeSnapshot struct {
	node        *RelayNode
	address     string
	ip          string
	score       float64
	successRate float64
	failCount   int
	failTime    time.Time
}

// NewRelayNodeManager 创建新的中转节点管理器
func NewRelayNodeManager() *RelayNodeManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RelayNodeManager{
		ctx:      ctx,
		cancel:   cancel,
		lookupIP: net.LookupIP,
	}
}

func splitAddress(address string, defaultPort string) (string, string) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address, defaultPort
	}
	return host, port
}

func formatIPPort(ip net.IP, port string) string {
	if ip.To4() == nil {
		return "[" + ip.String() + "]:" + port
	}
	return ip.String() + ":" + port
}

func (m *RelayNodeManager) newNode(sourceAddress string, resolvedAddr string) *RelayNode {
	return &RelayNode{
		ID:      resolvedAddr,
		Address: sourceAddress,
		IP:      resolvedAddr,
		Score:   50.0,
	}
}

func (m *RelayNodeManager) snapshotNode(node *RelayNode) relayNodeSnapshot {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return relayNodeSnapshot{
		node:        node,
		address:     node.Address,
		ip:          node.IP,
		score:       node.Score,
		successRate: node.SuccessRate,
		failCount:   node.FailCount,
		failTime:    node.FailTime,
	}
}

func (m *RelayNodeManager) snapshotNodes() []relayNodeSnapshot {
	m.mu.RLock()
	nodes := make([]*RelayNode, len(m.nodes))
	copy(nodes, m.nodes)
	m.mu.RUnlock()

	snapshots := make([]relayNodeSnapshot, 0, len(nodes))
	for _, node := range nodes {
		snapshots = append(snapshots, m.snapshotNode(node))
	}
	return snapshots
}

func (m *RelayNodeManager) addResolvedNodes(sourceAddress string, host string, port string, ips []net.IP) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := make(map[string]bool, len(m.nodes))
	for _, node := range m.nodes {
		existing[node.IP] = true
	}

	for _, ip := range ips {
		resolvedAddr := formatIPPort(ip, port)
		if existing[resolvedAddr] {
			continue
		}
		m.nodes = append(m.nodes, m.newNode(sourceAddress, resolvedAddr))
		existing[resolvedAddr] = true
	}
}

func (m *RelayNodeManager) refreshHostnameNodes() {
	snapshots := m.snapshotNodes()
	seenSource := make(map[string]bool)

	for _, snapshot := range snapshots {
		if seenSource[snapshot.address] {
			continue
		}
		seenSource[snapshot.address] = true

		host, port := splitAddress(snapshot.address, "443")
		if net.ParseIP(host) != nil {
			continue
		}

		ips, err := m.lookupIP(host)
		if err != nil {
			continue
		}
		m.addResolvedNodes(snapshot.address, host, port, ips)
	}
}

// AddNode 添加节点
func (m *RelayNodeManager) AddNode(address string, defaultPort string) error {
	host, port := splitAddress(address, defaultPort)

	if ip := net.ParseIP(host); ip != nil {
		resolvedAddr := formatIPPort(ip, port)
		node := m.newNode(resolvedAddr, resolvedAddr)
		m.mu.Lock()
		m.nodes = append(m.nodes, node)
		m.mu.Unlock()
		return nil
	}

	addrs, err := m.lookupIP(host)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ip := range addrs {
		resolvedAddr := formatIPPort(ip, port)
		m.nodes = append(m.nodes, m.newNode(address, resolvedAddr))
	}
	return nil
}

// AddNodeAndTest 添加节点并测试速度
func (m *RelayNodeManager) AddNodeAndTest(address string, defaultPort string) ([]string, error) {
	host, port := splitAddress(address, defaultPort)
	var addedIPs []string

	if ip := net.ParseIP(host); ip != nil {
		resolvedAddr := formatIPPort(ip, port)
		node := m.newNode(resolvedAddr, resolvedAddr)
		if err := m.TestNodeSpeed(node); err != nil {
			log.Printf("[中转节点] TCP连接测试失败: %s, 错误: %v (节点已加入列表,等待后台测速)", resolvedAddr, err)
			node.mu.Lock()
			node.Latency = 9999 * time.Second
			node.SuccessRate = 0.0
			node.mu.Unlock()
		} else {
			node.mu.Lock()
			node.SuccessRate = 1.0
			node.mu.Unlock()
			addedIPs = append(addedIPs, node.IP)
		}
		node.mu.Lock()
		node.LastTest = time.Now()
		node.Score = node.CalculateScore()
		node.Weight = node.Score
		node.mu.Unlock()

		m.mu.Lock()
		m.nodes = append(m.nodes, node)
		m.mu.Unlock()
		return addedIPs, nil
	}

	addrs, err := m.lookupIP(host)
	if err != nil {
		return nil, err
	}

	log.Printf("[中转节点] 域名 %s 解析到 %d 个IP地址", host, len(addrs))
	nodes := make([]*RelayNode, 0, len(addrs))
	for _, ip := range addrs {
		resolvedAddr := formatIPPort(ip, port)
		node := m.newNode(address, resolvedAddr)
		if err := m.TestNodeSpeed(node); err != nil {
			log.Printf("[中转节点] TCP连接测试失败: %s, 错误: %v (节点已加入列表,等待后台测速)", resolvedAddr, err)
			node.mu.Lock()
			node.Latency = 9999 * time.Second
			node.SuccessRate = 0.0
			node.mu.Unlock()
		} else {
			node.mu.Lock()
			node.SuccessRate = 1.0
			node.mu.Unlock()
			addedIPs = append(addedIPs, node.IP)
		}
		node.mu.Lock()
		node.LastTest = time.Now()
		node.Score = node.CalculateScore()
		node.Weight = node.Score
		node.mu.Unlock()
		nodes = append(nodes, node)
	}

	m.mu.Lock()
	m.nodes = append(m.nodes, nodes...)
	m.mu.Unlock()
	return addedIPs, nil
}

// TestNodeSpeed 测试节点速度
func (m *RelayNodeManager) TestNodeSpeed(node *RelayNode) error {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", node.IP, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	latency := time.Since(start)
	node.mu.Lock()
	node.Latency = latency
	node.mu.Unlock()
	return nil
}

// CalculateScore 计算节点评分
func (node *RelayNode) CalculateScore() float64 {
	maxLatency := 5000 * time.Millisecond
	normalizedLatency := float64(node.Latency) / float64(maxLatency)
	if normalizedLatency > 1.0 {
		normalizedLatency = 1.0
	}

	baseScore := (1.0-normalizedLatency)*0.7 + node.SuccessRate*0.3
	hoursSinceTest := time.Since(node.LastTest).Hours()
	decayFactor := 1.0
	if hoursSinceTest > 1 {
		decayFactor = 1.0 / (1.0 + hoursSinceTest*0.1)
	}
	return baseScore * decayFactor
}

// SelectBestNode 选择最佳节点
func (m *RelayNodeManager) SelectBestNode() *RelayNode {
	snapshots := m.snapshotNodes()
	if len(snapshots) == 0 {
		return nil
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].score > snapshots[j].score
	})
	return snapshots[0].node
}

// SelectBestNodes 选择最多n个最佳节点
func (m *RelayNodeManager) SelectBestNodes(n int) []*RelayNode {
	snapshots := m.snapshotNodes()
	if len(snapshots) == 0 {
		return nil
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].score > snapshots[j].score
	})
	if n > len(snapshots) {
		n = len(snapshots)
	}
	best := make([]*RelayNode, 0, n)
	for _, snapshot := range snapshots[:n] {
		best = append(best, snapshot.node)
	}
	return best
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
	log.Printf("[客户端] 执行初始节点测速...")
	m.testAllNodes()
	log.Printf("[客户端] 初始节点测速完成")

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
	m.refreshHostnameNodes()

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
				n.mu.Lock()
				n.Latency = 9999 * time.Second
				n.SuccessRate = 0.0
				n.mu.Unlock()
			} else {
				n.mu.Lock()
				n.SuccessRate = 1.0
				n.mu.Unlock()
			}
			n.mu.Lock()
			n.LastTest = time.Now()
			n.Score = n.CalculateScore()
			n.Weight = n.Score
			n.mu.Unlock()
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

// GetHealthyRelayIPs 获取当前健康的中转节点IP列表（评分>30且成功率>0）
func (m *RelayNodeManager) GetHealthyRelayIPs() []string {
	snapshots := m.snapshotNodes()
	var healthyIPs []string
	for _, snapshot := range snapshots {
		if snapshot.score > 30.0 && snapshot.successRate > 0.0 {
			healthyIPs = append(healthyIPs, snapshot.ip)
		}
	}
	return healthyIPs
}

// GetAvailableHealthyCount 获取可用健康节点数量（评分>30且成功率>0）
func (m *RelayNodeManager) GetAvailableHealthyCount() int {
	snapshots := m.snapshotNodes()
	count := 0
	for _, snapshot := range snapshots {
		if snapshot.score > 30.0 && snapshot.successRate > 0.0 {
			count++
		}
	}
	return count
}

// NodeCount 返回节点总数
func (m *RelayNodeManager) NodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}

// MarkNodeFailed 标记节点失败
func (m *RelayNodeManager) MarkNodeFailed(ip string) {
	node := m.GetNodeByIP(ip)
	if node == nil {
		return
	}

	node.mu.Lock()
	node.FailCount++
	failCount := node.FailCount
	node.FailTime = time.Now()
	node.SuccessRate = 0.0
	node.Score = 0.0
	node.Weight = 0.0
	node.mu.Unlock()
	log.Printf("[中转节点] 节点 %s 标记失败 (连续失败: %d)", ip, failCount)
}

// MarkNodeSuccess 标记节点成功
func (m *RelayNodeManager) MarkNodeSuccess(ip string) {
	node := m.GetNodeByIP(ip)
	if node == nil {
		return
	}

	node.mu.Lock()
	node.FailCount = 0
	node.SuccessRate = 1.0
	node.LastTest = time.Now()
	node.Score = node.CalculateScore()
	node.Weight = node.Score
	node.mu.Unlock()
}

// SelectNodeExcluding 申请1个新节点,排除指定的IP列表
func (m *RelayNodeManager) SelectNodeExcluding(excludeIPs []string) *RelayNode {
	excludeMap := make(map[string]bool)
	for _, ip := range excludeIPs {
		excludeMap[ip] = true
	}

	selectHealthy := func() []relayNodeSnapshot {
		snapshots := m.snapshotNodes()
		var candidates []relayNodeSnapshot
		now := time.Now()
		for _, snapshot := range snapshots {
			if excludeMap[snapshot.ip] {
				continue
			}
			if snapshot.failCount >= 3 && now.Sub(snapshot.failTime) < 30*time.Second {
				continue
			}
			if snapshot.successRate <= 0.0 {
				continue
			}
			candidates = append(candidates, snapshot)
		}
		return candidates
	}

	candidates := selectHealthy()
	if len(candidates) == 0 {
		m.refreshHostnameNodes()
		candidates = selectHealthy()
	}
	if len(candidates) > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].score > candidates[j].score
		})
		return candidates[0].node
	}

	fallback := m.snapshotNodes()
	var filtered []relayNodeSnapshot
	for _, snapshot := range fallback {
		if excludeMap[snapshot.ip] {
			continue
		}
		filtered = append(filtered, snapshot)
	}
	if len(filtered) == 0 {
		return nil
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].failCount != filtered[j].failCount {
			return filtered[i].failCount < filtered[j].failCount
		}
		if filtered[i].score != filtered[j].score {
			return filtered[i].score > filtered[j].score
		}
		return filtered[i].ip < filtered[j].ip
	})
	return filtered[0].node
}
