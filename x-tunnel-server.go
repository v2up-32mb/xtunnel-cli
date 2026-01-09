//go:build server
// +build server

package main

import (
	"crypto/tls"
	"flag"
	"log"
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


