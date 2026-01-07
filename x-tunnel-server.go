//go:build server
// +build server

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"log"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var (
	listenAddr string
	token      string
	certFile   string
	keyFile    string
	upgrader   = websocket.Upgrader{
		ReadBufferSize:  64 * 1024,
		WriteBufferSize: 64 * 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func init() {
	flag.StringVar(&listenAddr, "l", ":8443", "监听地址")
	flag.StringVar(&token, "t", "", "认证令牌")
	flag.StringVar(&certFile, "cert", "", "TLS 证书文件 (不指定则自动生成自签证书)")
	flag.StringVar(&keyFile, "key", "", "TLS 私钥文件 (不指定则自动生成自签证书)")
}

func main() {
	flag.Parse()

	if token == "" {
		log.Fatal("[服务端] 错误: 必须指定 -token 参数")
	}

	pool := NewServerPool(token)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		pool.handleWebSocket(w, r)
	})

	var tlsConfig *tls.Config

	// 如果指定了证书文件，使用指定证书；否则生成自签证书
	if certFile != "" && keyFile != "" {
		log.Printf("[服务端] 使用指定证书: %s", certFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalf("[服务端] 加载证书失败: %v", err)
		}
		tlsConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
	} else {
		log.Printf("[服务端] 自动生成自签证书")
		cert, err := generateSelfSignedCert()
		if err != nil {
			log.Fatalf("[服务端] 生成证书失败: %v", err)
		}
		tlsConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
	}

	server := &http.Server{
		Addr:      listenAddr,
		TLSConfig: tlsConfig,
	}

	log.Printf("[服务端] HTTPS 监听: %s", listenAddr)
	log.Printf("[服务端] Token: %s", token)

	if err := server.ListenAndServeTLS("", ""); err != nil {
		log.Fatal("[服务端]", err)
	}
}

// generateSelfSignedCert 生成自签证书
func generateSelfSignedCert() (tls.Certificate, error) {
	// 生成 RSA 私钥
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	// 设置证书模板
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"x-tunnel"},
			CommonName:   "x-tunnel-server",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // 1 年有效期
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	// 生成证书 DER 编码
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	// 将证书和私钥转换为 PEM 格式
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	privPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})

	// 加载证书
	cert, err := tls.X509KeyPair(certPEM, privPEM)
	if err != nil {
		return tls.Certificate{}, err
	}

	return cert, nil
}
