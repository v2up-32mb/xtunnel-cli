package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const typeHTTPS = 65

// ECHManager ECH 管理器
type ECHManager struct {
	config      *Config
	echList     []byte
	echListMu   sync.RWMutex
	refreshMu   sync.Mutex
}

// NewECHManager 创建 ECH 管理器
func NewECHManager(cfg *Config) *ECHManager {
	return &ECHManager{
		config: cfg,
	}
}

// Prepare 获取 ECH 配置
func (m *ECHManager) Prepare() error {
	for {
		log.Printf("[客户端] DNS查询 ECH: %s -> %s", m.config.DNSServer, m.config.ECHDomain)
		echBase64, err := m.queryHTTPSRecord(m.config.ECHDomain, m.config.DNSServer)
		if err != nil {
			log.Printf("[客户端] DNS 查询失败: %v，重试...", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if echBase64 == "" {
			log.Printf("[客户端] 未找到 ECH 参数，重试...")
			time.Sleep(2 * time.Second)
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(echBase64)
		if err != nil {
			log.Printf("[客户端] ECH Base64 解码失败: %v，重试...", err)
			time.Sleep(2 * time.Second)
			continue
		}
		m.echListMu.Lock()
		m.echList = raw
		m.echListMu.Unlock()
		log.Printf("[客户端] ECHConfigList 长度: %d 字节", len(raw))
		return nil
	}
}

// Refresh 刷新 ECH 配置
func (m *ECHManager) Refresh() error {
	if !m.config.EnableECH {
		return nil
	}

	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	m.echListMu.RLock()
	if len(m.echList) > 0 {
		m.echListMu.RUnlock()
		return nil
	}
	m.echListMu.RUnlock()

	log.Printf("[客户端] 刷新 ECH 配置...")
	return m.Prepare()
}

// GetList 获取当前 ECH 配置
func (m *ECHManager) GetList() ([]byte, error) {
	if !m.config.EnableECH {
		return nil, nil
	}
	m.echListMu.RLock()
	defer m.echListMu.RUnlock()
	if len(m.echList) == 0 {
		return nil, errors.New("ECH 配置尚未加载")
	}
	return m.echList, nil
}

// BuildTLSConfig 构建 TLS 配置
func (m *ECHManager) BuildTLSConfig(serverName string) (*tls.Config, error) {
	if !m.config.EnableECH {
		return m.buildStandardTLSConfig(serverName)
	}

	ech, e := m.GetList()
	if e != nil {
		return nil, e
	}
	cfgTLS, err := m.buildTLSConfigWithECH(serverName, ech)
	if err != nil {
		return nil, err
	}
	cfgTLS.InsecureSkipVerify = m.config.InsecureSkipVerify
	return cfgTLS, nil
}

// buildTLSConfigWithECH 构建 TLS 配置（启用 ECH）
func (m *ECHManager) buildTLSConfigWithECH(serverName string, echList []byte) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:                     tls.VersionTLS13,
		ServerName:                     serverName,
		EncryptedClientHelloConfigList: echList,
		EncryptedClientHelloRejectionVerify: func(cs tls.ConnectionState) error {
			return errors.New("服务器拒绝 ECH")
		},
		RootCAs: roots,
	}, nil
}

// buildStandardTLSConfig 构建 TLS 配置（禁用 ECH）
func (m *ECHManager) buildStandardTLSConfig(serverName string) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		RootCAs:            roots,
		InsecureSkipVerify: m.config.InsecureSkipVerify,
	}, nil
}

// queryHTTPSRecord 查询 HTTPS 记录
func (m *ECHManager) queryHTTPSRecord(domain, dnsServer string) (string, error) {
	if strings.HasPrefix(dnsServer, "http://") || strings.HasPrefix(dnsServer, "https://") {
		return m.queryDoH(domain, dnsServer)
	}
	return m.queryDNSUDP(domain, dnsServer)
}

// queryDNSUDP 通过 UDP 查询 DNS
func (m *ECHManager) queryDNSUDP(domain, dnsServer string) (string, error) {
	if !strings.Contains(dnsServer, ":") {
		dnsServer = dnsServer + ":53"
	}
	query := buildDNSQuery(domain, typeHTTPS)

	conn, err := net.Dial("udp", dnsServer)
	if err != nil {
		return "", fmt.Errorf("连接 DNS 服务器失败: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err = conn.Write(query); err != nil {
		return "", fmt.Errorf("发送查询失败: %v", err)
	}

	response := make([]byte, 4096)
	n, err := conn.Read(response)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return "", fmt.Errorf("DNS 查询超时")
		}
		return "", fmt.Errorf("读取 DNS 响应失败: %v", err)
	}
	return parseDNSResponse(response[:n])
}

// queryDoH 通过 DoH 查询 DNS
func (m *ECHManager) queryDoH(domain, dohURL string) (string, error) {
	u, err := url.Parse(dohURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	dnsQuery := buildDNSQuery(domain, typeHTTPS)
	dnsBase64 := base64.RawURLEncoding.EncodeToString(dnsQuery)
	q.Set("dns", dnsBase64)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/dns-message")
	req.Header.Set("Content-Type", "application/dns-message")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DoH 状态码: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return parseDNSResponse(body)
}

// buildDNSQuery 构建 DNS 查询报文
func buildDNSQuery(domain string, qtype uint16) []byte {
	query := make([]byte, 0, 512)
	// ID=0x0001, RD=1
	query = append(query, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00)
	for _, label := range strings.Split(domain, ".") {
		query = append(query, byte(len(label)))
		query = append(query, []byte(label)...)
	}
	query = append(query, 0x00)
	query = append(query, byte(qtype>>8), byte(qtype), 0x00, 0x01)
	return query
}

// parseDNSResponse 解析 DNS 响应
func parseDNSResponse(response []byte) (string, error) {
	if len(response) < 12 {
		return "", fmt.Errorf("响应过短")
	}
	ancount := binary.BigEndian.Uint16(response[6:8])
	if ancount == 0 {
		return "", fmt.Errorf("无答案记录")
	}

	offset := 12
	for offset < len(response) && response[offset] != 0 {
		offset += int(response[offset]) + 1
	}
	offset += 5 // 0 + QTYPE(2) + QCLASS(2)

	for i := 0; i < int(ancount); i++ {
		if offset >= len(response) {
			break
		}
		// NAME: pointer or labels
		if response[offset]&0xC0 == 0xC0 {
			offset += 2
		} else {
			for offset < len(response) && response[offset] != 0 {
				offset += int(response[offset]) + 1
			}
			offset++
		}
		if offset+10 > len(response) {
			break
		}
		rrType := binary.BigEndian.Uint16(response[offset : offset+2])
		offset += 8 // TYPE(2)+CLASS(2)+TTL(4)
		dataLen := binary.BigEndian.Uint16(response[offset : offset+2])
		offset += 2
		if offset+int(dataLen) > len(response) {
			break
		}
		data := response[offset : offset+int(dataLen)]
		offset += int(dataLen)
		if rrType == typeHTTPS {
			if ech := parseHTTPSRecord(data); ech != "" {
				return ech, nil
			}
		}
	}
	return "", nil
}

// parseHTTPSRecord 解析 HTTPS 记录中的 ECH 配置
func parseHTTPSRecord(data []byte) string {
	if len(data) < 2 {
		return ""
	}
	offset := 2 // priority
	// targetName (root=0) or labels
	if offset < len(data) && data[offset] == 0 {
		offset++
	} else {
		for offset < len(data) && data[offset] != 0 {
			offset += int(data[offset]) + 1
		}
		offset++
	}
	// params: key(2) len(2) value(len)
	for offset+4 <= len(data) {
		key := binary.BigEndian.Uint16(data[offset : offset+2])
		length := binary.BigEndian.Uint16(data[offset+2 : offset+4])
		offset += 4
		if offset+int(length) > len(data) {
			break
		}
		value := data[offset : offset+int(length)]
		offset += int(length)
		// ECHConfigList is parameter key=5
		if key == 5 {
			return base64.StdEncoding.EncodeToString(value)
		}
	}
	return ""
}
