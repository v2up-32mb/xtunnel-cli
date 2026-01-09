//go:build client

package main

import (
	"net"
	"strings"
)

// parseIPStrategy 解析 IP 策略字符串
//
// 支持的格式:
//   - "" 或 "default": IPStrategyDefault
//   - "4": IPStrategyIPv4Only
//   - "6": IPStrategyIPv6Only
//   - "4,6": IPStrategyPv4Pv6
//   - "6,4": IPStrategyPv6Pv4
//
// 参数:
//   - s: 策略字符串
//
// 返回策略代码
func parseIPStrategy(s string) byte {
	s = strings.ReplaceAll(strings.TrimSpace(s), " ", "")
	switch s {
	case "4":
		return IPStrategyIPv4Only
	case "6":
		return IPStrategyIPv6Only
	case "4,6":
		return IPStrategyPv4Pv6
	case "6,4":
		return IPStrategyPv6Pv4
	default:
		return IPStrategyDefault
	}
}

// resolveWithStrategy 根据 IP 策略解析目标地址
//
// 参数:
//   - target: 目标地址（host:port 格式）
//   - strategy: IP 策略
//
// 返回格式化后的地址（IPv6 地址会自动添加括号）
func resolveWithStrategy(target string, strategy byte) string {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return target
	}

	// 如果已经是纯 IP 地址，直接返回
	if ip := net.ParseIP(host); ip != nil {
		// IPv6 地址需要加括号
		if ip.To4() == nil && !strings.HasPrefix(host, "[") {
			return "[" + host + "]:" + port
		}
		return target
	}

	// 根据策略解析域名
	switch strategy {
	case IPStrategyIPv4Only:
		return resolveIPv4Only(host, port)
	case IPStrategyIPv6Only:
		return resolveIPv6Only(host, port)
	case IPStrategyPv4Pv6:
		return resolveIPv4First(host, port)
	case IPStrategyPv6Pv4:
		return resolveIPv6First(host, port)
	default:
		return target // 系统默认
	}
}

// resolveIPv4Only 仅返回 IPv4 地址
//
// 如果域名没有 IPv4 地址，返回原始 host:port
func resolveIPv4Only(host, port string) string {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return host + ":" + port
	}
	for _, addr := range addrs {
		if addr.To4() != nil {
			return addr.String() + ":" + port
		}
	}
	return host + ":" + port
}

// resolveIPv6Only 仅返回 IPv6 地址
//
// 如果域名没有 IPv6 地址，返回原始 [host]:port
func resolveIPv6Only(host, port string) string {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return "[" + host + "]:" + port
	}
	for _, addr := range addrs {
		if addr.To4() == nil {
			return "[" + addr.String() + "]:" + port
		}
	}
	return "[" + host + "]:" + port
}

// resolveIPv4First 优先返回 IPv4 地址
//
// 如果没有 IPv4 地址，则尝试 IPv6
func resolveIPv4First(host, port string) string {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return host + ":" + port
	}
	// 先找 IPv4
	for _, addr := range addrs {
		if addr.To4() != nil {
			return addr.String() + ":" + port
		}
	}
	// 再找 IPv6
	for _, addr := range addrs {
		if addr.To4() == nil {
			return "[" + addr.String() + "]:" + port
		}
	}
	return host + ":" + port
}

// resolveIPv6First 优先返回 IPv6 地址
//
// 如果没有 IPv6 地址，则尝试 IPv4
func resolveIPv6First(host, port string) string {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return "[" + host + "]:" + port
	}
	// 先找 IPv6
	for _, addr := range addrs {
		if addr.To4() == nil {
			return "[" + addr.String() + "]:" + port
		}
	}
	// 再找 IPv4
	for _, addr := range addrs {
		if addr.To4() != nil {
			return addr.String() + ":" + port
		}
	}
	return "[" + host + "]:" + port
}
