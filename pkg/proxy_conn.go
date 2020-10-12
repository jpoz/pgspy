package pgspy

import (
	"net"

	log "github.com/sirupsen/logrus"
)

// ProxyConn - Proxy connection, piping data between proxy and remote.
type ProxyConn struct {
	sentBytes     uint64
	receivedBytes uint64
	laddr, raddr  *net.TCPAddr
	lconn, rconn  *net.TCPConn
	erred         bool
	errsig        chan bool
	prefix        string
	connID        uint64
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
		b := getModifiedBuffer(buff[:n], callback)

		n, err = dst.Write(b)
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
		b := setResponseBuffer(pc.erred, buff[:n], callback)

		n, err = dst.Write(b)
		if err != nil {
			log.Errorf("Write failed '%s'\n", err)
			return
		}
	}
}

// ModifiedBuffer when is local and will call filterCallback function
func getModifiedBuffer(buffer []byte, filterCallback Callback) (b []byte) {
	go filterCallback(buffer)

	return buffer
}

// ResponseBuffer when is local and will call returnCallback function
func setResponseBuffer(iserr bool, buffer []byte, filterCallback Callback) (b []byte) {
	go filterCallback(buffer)

	return buffer
}
