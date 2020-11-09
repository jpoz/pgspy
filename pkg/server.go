package pgspy

import (
	log "github.com/sirupsen/logrus"
)

// Start will start a new server
func Start() {
	pgHost := "127.0.0.1:5432"
	proxyHost := "127.0.0.1:5543"

	proxy := NewProxy(pgHost, proxyHost)
	proxy.OnMessage = func(msg PostgresMessage) {
		if msg.Outgoing {
			log.Infof("-> %s \n %s \n", msg.TypeIdentifier, msg.Payload)
		} else {
			log.Infof("<- %s \n %s \n", msg.TypeIdentifier, msg.Payload)
		}
	}
	proxy.Start()
}
