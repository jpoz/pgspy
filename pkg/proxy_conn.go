package pgspy

import (
	"fmt"
	"net"
	"os"
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
func (pc *ProxyConn) Pipe(before Callback, after Callback) {
	defer pc.lconn.Close()

	// connect to remote server
	rconn, err := net.DialTCP("tcp", nil, pc.raddr)
	if err != nil {
		log.Errorf("Failed to connect to Postgres: %s", err)
		return
	}
	pc.rconn = rconn
	defer pc.rconn.Close()

	// proxying data
	go pc.handleIncomingConnection(pc.lconn, pc.rconn, before)
	go pc.handleResponseConnection(pc.rconn, pc.lconn, after)

	// wait for close...
	<-pc.errsig
}

// Proxy.handleIncomingConnection
func (pc *ProxyConn) handleIncomingConnection(src, dst *net.TCPConn, callback Callback) {
	// directional copy (64k buffer)
	buff := make([]byte, 0xffff)

	for {
		n, err := src.Read(buff)
		if err != nil {
			log.Errorf("Read failed '%s'\n", err)
			return
		}

		msgID := atomic.AddUint64(&pc.msgID, 1)
		var cbuff = make([]byte, n)
		copy(cbuff, buff[:n])

		wireMsg := WireMessage{
			Buff:     cbuff,
			MsgID:    msgID,
			Outgoing: false,
		}
		pc.parser.Incoming <- wireMsg

		n, err = dst.Write(buff[:n])
		if err != nil {
			log.Errorf("Write failed '%s'\n", err)
			return
		}
	}
}

// Proxy.handleResponseConnection
func (pc *ProxyConn) handleResponseConnection(src, dst *net.TCPConn, callback Callback) {
	// directional copy (64k buffer)
	buff := make([]byte, 0xffff)

	for {
		n, err := src.Read(buff)
		if err != nil {
			log.Errorf("Read failed '%s'\n", err)
			return
		}

		msgID := atomic.AddUint64(&pc.msgID, 1)
		var cbuff = make([]byte, n)
		copy(cbuff, buff[:n])

		wireMsg := WireMessage{
			Buff:     cbuff,
			MsgID:    msgID,
			Outgoing: true,
		}
		pc.parser.Incoming <- wireMsg

		n, err = dst.Write(buff[:n])
		if err != nil {
			log.Errorf("Write failed '%s'\n", err)
			return
		}
	}
}

func writeBuffer(buffer []byte, msgID uint64, prefix string) {
	fmt.Printf("WRITING pkg/testdata/%s-%03d\n", prefix, msgID)
	f, err := os.Create(fmt.Sprintf("pkg/testdata/%s-%03d", prefix, msgID))
	if err != nil {
		fmt.Println(f)
	}
	defer f.Close()

	_, err = f.Write(buffer)
	if err != nil {
		fmt.Println(f)
	}
}

// ModifiedBuffer when is local and will call filterCallback function
func getModifiedBuffer(buffer []byte, filterCallback Callback, msgID uint64) (b []byte) {
	go filterCallback(buffer, msgID)

	return buffer
}

// ResponseBuffer when is local and will call returnCallback function
func setResponseBuffer(iserr bool, buffer []byte, filterCallback Callback, msgID uint64) (b []byte) {
	go filterCallback(buffer, msgID)

	return buffer
}
