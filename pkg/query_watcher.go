package pgspy

import (
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

// QueryWatcher will print out queries, how long they took and how many rows were returned
type QueryWatcher struct {
	currentQuery string
	bindings     []string
	dataRowCount uint64
}

// OnMessage consumes messages from a server
func (qw *QueryWatcher) OnMessage(msg PostgresMessage) {
	if msg.Outgoing {
		// log.Infof("-> %s\n", msg.TypeIdentifier)
	} else {
		// log.Infof("<- %s \n %s \n", msg.TypeIdentifier, msg.Payload)
	}

	switch msg.TypeIdentifier {
	case QueryIncoming, ParseIncoming:
		// TODO parse and make pretty
		qw.currentQuery = string(msg.Payload)
		qw.dataRowCount = 0
		return
	case BindIncoming:
		qw.bindings = append(qw.bindings, string(msg.Payload))
	case DataRowOutgoing:
		atomic.AddUint64(&qw.dataRowCount, 1)
		return
	case CommandCompleteOutgoing:
		log.Infof("%s\nBinding: %v\nRows: %d", qw.currentQuery, qw.bindings, qw.dataRowCount)
		qw.currentQuery = ""
		qw.bindings = []string{}
		return
	}
}
