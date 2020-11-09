package pgspy

// Start will start a new server
func Start() {
	pgHost := "127.0.0.1:5432"
	proxyHost := "127.0.0.1:5543"

	proxy := NewProxy(pgHost, proxyHost)
	queryWatcher := QueryWatcher{}
	proxy.OnMessage = queryWatcher.OnMessage
	// proxy.OnMessage = func(msg PostgresMessage) {
	// 	if msg.Outgoing {
	// 		log.Infof("-> %s\n", msg.TypeIdentifier)
	// 	} else {
	// 		log.Infof("<- %s \n %s \n", msg.TypeIdentifier, msg.Payload)
	// 	}
	// }
	proxy.Start()
}
