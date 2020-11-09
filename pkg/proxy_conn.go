package pgspy

import (
	"net"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

// ProxyConn - Proxy connection, piping data between proxy and remote.
type ProxyConn struct {
	laddr, raddr *net.TCPAddr
	lconn, rconn *net.TCPConn
	erred        bool
	errsig       chan bool
	prefix       string
	connID       uint64
	msgID        uint64
	parser       *Parser
}

// Pipe will move the bites from the proxy to postgres and back
func (pc *ProxyConn) Pipe() {
	defer pc.lconn.Close()

	// connect to remote server
	rconn, err := net.DialTCP("tcp", nil, pc.raddr)
	if err != nil {
		log.Errorf("Failed to connect to Postgres: %s", err)
		return
	}
	pc.rconn = rconn
	defer pc.rconn.Close()

	// run parser
	go pc.parser.Parse()

	// proxying data
	go pc.handleRequestConnection(pc.lconn, pc.rconn)
	go pc.handleResponseConnection(pc.rconn, pc.lconn)

	// wait for close...
	<-pc.errsig
}

// Proxy.handleIncomingConnection
func (pc *ProxyConn) handleRequestConnection(src, dst *net.TCPConn) {
	// directional copy (64k buffer)
	buff := make([]byte, 0xffff)

	for {
		n, err := src.Read(buff)
		if err != nil {
			log.Errorf("Read failed '%s'\n", err)
			return
		}

		msgID := atomic.AddUint64(&pc.msgID, 1)
		go pc.sendMessageToParser(buff[:n], msgID, false)

		n, err = dst.Write(buff[:n])
		if err != nil {
			log.Errorf("Write failed '%s'\n", err)
			return
		}
	}
}

// Proxy.handleResponseConnection
func (pc *ProxyConn) handleResponseConnection(src, dst *net.TCPConn) {
	// directional copy (64k buffer)
	buff := make([]byte, 0xffff)

	for {
		n, err := src.Read(buff)
		if err != nil {
			log.Errorf("Read failed '%s'\n", err)
			return
		}

		msgID := atomic.AddUint64(&pc.msgID, 1)
		go pc.sendMessageToParser(buff[:n], msgID, true)

		n, err = dst.Write(buff[:n])
		if err != nil {
			log.Errorf("Write failed '%s'\n", err)
			return
		}
	}
}

func (pc *ProxyConn) sendMessageToParser(buffer []byte, msgID uint64, outgoing bool) {
	wireMsg := WireMessage{
		Buff:     buffer,
		MsgID:    msgID,
		Outgoing: outgoing,
	}
	pc.parser.Incoming <- wireMsg
}
