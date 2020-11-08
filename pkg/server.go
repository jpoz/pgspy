package pgspy

// Start will start a new server
func Start() {
	pgHost := "127.0.0.1:5432"
	proxyHost := "127.0.0.1:5543"

	proxy := NewProxy(pgHost, proxyHost)
	proxy.Before = func(b []byte, id uint64) {
		ParseIncoming(b, id)
	}

	proxy.After = func(b []byte, id uint64) {
		ParseOutgoing(b, id)
	}
	proxy.Start()
}
