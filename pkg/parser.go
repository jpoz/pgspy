package pgspy

import (
	"encoding/binary"

	log "github.com/sirupsen/logrus"
)

// IncomingTypes is a map from the charTag to the message type
var IncomingTypes = map[rune]string{
	'B': "Bind",
	'C': "Close",
	'd': "CopyData",
	'c': "CopyDone",
	'f': "CopyFail",
	'D': "Describe",
	'E': "Execute",
	'H': "Flush",
	'F': "FunctionCall",
	'P': "Parse",
	'p': "PasswordMessage",
	'Q': "Query",
	'S': "Sync",
	'X': "Terminate",
}

// OutoingType is a map from the charTag to the message type
var OutoingType = map[rune]string{
	'7': "Unknown",
	'R': "Authentication",
	'K': "BackendKeyData",
	'2': "BindComplete",
	'3': "CloseComplete",
	'C': "CommandComplete",
	'd': "CopyData",
	'c': "CopyDone",
	'G': "CopyInResponse",
	'H': "CopyOutResponse",
	'W': "CopyBothResponse",
	'D': "DataRow",
	'I': "EmptyQueryResponse",
	'E': "ErrorResponse",
	'V': "FunctionCallResponse",
	'v': "NegotiateProtocolVersion",
	'n': "NoData",
	'N': "NoticeResponse",
	'A': "NotificationResponse",
	't': "ParameterDescription",
	'S': "ParameterStatus",
	'1': "ParseComplete",
	's': "PortalSuspended",
	'Z': "ReadyForQuery",
	'T': "RowDescription",
}

// PostgresMessage represents a message between postgres and the proxy
type PostgresMessage interface {
	Print()
}

// ParseIncoming reads from bytes and returns the appropriate postgres message type
func ParseIncoming(buff []byte) PostgresMessage {
	// log.Infof("INCOMING <- %v", buff)
	if buff[0] == 0 {
		return parseIncomingStartupPacket(buff)
	}

	return parseIncomingPacket(buff)
}

// ParseOutgoing reads from bytes and returns the appropriate postgres message type
func ParseOutgoing(buff []byte) PostgresMessage {
	// log.Infof("OUTGOING -> %v", buff)
	if buff[0] == 0 {
		return parseOutgoingStartupPacket(buff)
	}

	return parseOutgoingPacket(buff)
}

func parseIncomingStartupPacket(buff []byte) PostgresMessage {

	return nil
}

func parseIncomingPacket(buff []byte) PostgresMessage {
	for len(buff) > 0 {
		charTag := rune(buff[0])
		length := binary.BigEndian.Uint32(buff[1:5])
		if len(buff) < int(length) {
			log.Errorf("Parse error, length too long\n%v", buff)

			return nil
		}

		payload := buff[5 : length+1]
		desc := IncomingTypes[charTag]

		log.Infof("<- %s (%d) %s", desc, length, payload)
		buff = buff[length+1:]
	}

	return nil
}

func parseOutgoingStartupPacket(buff []byte) PostgresMessage {
	return nil
}

func parseOutgoingPacket(buff []byte) PostgresMessage {
	for len(buff) > 0 {
		charTag := rune(buff[0])
		desc := OutoingType[charTag]

		if desc == "" {
			log.Errorf("Failed to find outgoing type %d\n%v", buff[0], buff)
			return nil
		}

		if len(buff) > 5 {
			// log.Infof("PROCESSING %v", buff)
			length := binary.BigEndian.Uint32(buff[1:5])
			// log.Infof("LENGTH %d", length)

			payload := buff[5 : length+1]

			log.Infof("-> %s (%d) %s", desc, length, payload)
			buff = buff[length+1:]
		} else {
			log.Infof("-> %s", desc)
			buff = []byte{}
		}
	}

	return nil
}
