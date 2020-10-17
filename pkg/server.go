package pgspy

import (
	"fmt"
)

func filterCallback(get []byte) bool {
	fmt.Printf("filter %s\n", get)
	return true
}

func returnCallBack(get []byte) bool {
	fmt.Printf("filter %s\n", get)
	return true
}

// Start will start a new server
func Start() {
	pgHost := "127.0.0.1:5432"
	proxyHost := "127.0.0.1:5543"

	proxy := NewProxy(pgHost, proxyHost)
	proxy.Before = func(b []byte) {
		// log.Infof("Before: %v -> %s ->", b[0:4], b)

		msg := ParseIncoming(b)
		if msg != nil {
			msg.Print()
		}
	}

	proxy.After = func(b []byte) {
		// log.Infof("After: %v -> %s ->", b[0:4], b)
		msg := ParseOutgoing(b)
		if msg != nil {
			msg.Print()
		}
	}
	proxy.Start()
}
