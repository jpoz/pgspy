package pgspy

import (
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
)

// NewProxy - initializes a proxy
func NewProxy(postgresAddr, proxyAddr string) *Proxy {
	return &Proxy{
		PostgresAddr: postgresAddr,
		ProxyAddr:    proxyAddr,
		Before:       func([]byte) {},
		After:        func([]byte) {},
		connid:       0,
	}
}

// Callback - function run before or after postgres
type Callback func(get []byte)

// Proxy - will proxy data to postgres
type Proxy struct {
	PostgresAddr string
	ProxyAddr    string
	Before       Callback
	After        Callback

	connid uint64
}

// Start - will start listening on the proxy port and forward data to the postgres port
func (p *Proxy) Start() {
	log.Infof("proxy %s -> postgres: %s", p.ProxyAddr, p.PostgresAddr)

	postgresAddr := ResolvedAddress(p.PostgresAddr)
	proxyAddr := ResolvedAddress(p.ProxyAddr)

	listener, err := net.ListenTCP("tcp", proxyAddr)
	if err != nil {
		log.Fatalf("ListenTCP of %s error:%v", proxyAddr, err)
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Errorf("Failed to accept connection '%s'\n", err)
			continue
		}
		p.connid++

		proxyConn := &ProxyConn{
			lconn:  conn,
			laddr:  proxyAddr,
			raddr:  postgresAddr,
			erred:  false,
			errsig: make(chan bool),
			prefix: fmt.Sprintf("Connection #%03d ", p.connid),
			connID: p.connid,
		}
		log.Printf("New connection #%03d", proxyConn.connID)
		go proxyConn.Pipe(p.Before, p.After)
	}
}
