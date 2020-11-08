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

// WireMessage contains the bytes that were transmitted over the wire and a unique incrementing id
type WireMessage struct {
	Buff     []byte
	MsgID    uint64
	Outgoing bool
}

// PostgresMessage represents a message between postgres and the proxy
type PostgresMessage struct {
	TypeIdentifier string
	Payload        string
}

// Parser keeps the state of buffer messages
type Parser struct {
	Incoming chan WireMessage
	Outgoing chan PostgresMessage

	currentMessage PostgresMessage
	leftBytes      int64
}

// ConsumeBuffers will parse WireMessage and send outgoing PostgresMessage on the outgoing buffer
func ConsumeBuffers(incoming chan WireMessage, outgoing chan PostgresMessage) {
	for {
		wireMsg := <-incoming
		buff := wireMsg.Buff
		msgID := wireMsg.MsgID

		if wireMsg.Outgoing {
			log.Infof("OUTGOING <- %v, %03d", buff[:10], msgID)
		} else {
			log.Infof("INCOMING <- %v, %03d", buff[:10], msgID)
		}
	}
}

// ParseIncoming reads from bytes and returns the appropriate postgres message type
func ParseIncoming(buff []byte, msgID uint64) []PostgresMessage {
	log.Infof("INCOMING <- %v, %03d", buff[:10], msgID)
	if buff[0] == 0 {
		return parseIncomingStartupPacket(buff)
	}

	return parseIncomingPacket(buff)
}

// ParseOutgoing reads from bytes and returns the appropriate postgres message type
func ParseOutgoing(buff []byte, msgID uint64) []PostgresMessage {
	log.Infof("OUTGOING -> %v, %03d", buff[:10], msgID)
	if buff[0] == 0 {
		return parseOutgoingStartupPacket(buff)
	}

	return parseOutgoingPacket(buff)
}

func parseIncomingStartupPacket(buff []byte) []PostgresMessage {
	log.Infof("<- StartupPacket")
	return nil
}

func parseIncomingPacket(buff []byte) []PostgresMessage {
	for len(buff) > 0 {
		charTag := rune(buff[0])
		length := binary.BigEndian.Uint32(buff[1:5])
		if len(buff) < int(length) {
			log.Errorf("Parse error, length too long\n")

			return nil
		}

		payload := buff[5 : length+1]
		desc := IncomingTypes[charTag]

		log.Infof("<- %s (%d) %s", desc, length, payload)
		buff = buff[length+1:]
	}

	return nil
}

func parseOutgoingStartupPacket(buff []byte) []PostgresMessage {
	log.Infof("-> StartupPacket")
	return nil
}

func parseOutgoingPacket(buff []byte) []PostgresMessage {
	for len(buff) > 0 {
		charTag := rune(buff[0])
		desc := OutoingType[charTag]

		if desc == "" {
			log.Errorf("Failed to find outgoing type %d\n", buff[0])
			return nil
		}

		if len(buff) > 5 {
			// log.Infof("PROCESSING %v", buff)
			length := binary.BigEndian.Uint32(buff[1:5])
			// log.Infof("LENGTH %d", length)
			if len(buff) < int(length) {
				log.Errorf("Parse error, length too long\n")

				return nil
			}

			payload := buff[5 : length+1]

			log.Infof("-> %s (%d) %s", desc, length, payload)
			buff = buff[length+1:]
		} else {
			// log.Infof("-> %s", desc)
			buff = []byte{}
		}
	}

	return nil
}
