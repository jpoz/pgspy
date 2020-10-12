package pgspy

import (
	"fmt"

	log "github.com/sirupsen/logrus"
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
		// log.Infof("Before: %s\n", b)
	}

	proxy.After = func(b []byte) {
		log.Infof("After: %v\n", b)
	}
	proxy.Start()
}
