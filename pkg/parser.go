package pgspy

import (
	"encoding/binary"
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
	TypeRune       rune
	Payload        []byte
	Outgoing       bool
}

// Parser keeps the state of buffer messages
type Parser struct {
	Incoming chan WireMessage
	Outgoing chan PostgresMessage

	currentRequestMessage *PostgresMessage
	remainingRequestBytes uint32

	currentResponseMessage *PostgresMessage
	remainingResponseBytes uint32
}

// Parse will parse WireMessage and send outgoing PostgresMessage on the outgoing buffer
func (p *Parser) Parse() {
	p.currentRequestMessage = &PostgresMessage{Outgoing: false}
	p.currentResponseMessage = &PostgresMessage{Outgoing: true}

	for {
		wireMsg := <-p.Incoming
		buff := wireMsg.Buff
		msgID := wireMsg.MsgID

		if wireMsg.Outgoing {
			// log.Infof("OUTGOING -> %v, %03d", buff[:10], msgID)
			p.ParseOutgoing(buff, msgID)
		} else {
			// log.Infof("INCOMING <- %v, %03d", buff[:10], msgID)
			p.ParseIncoming(buff, msgID)
		}
	}
}

// ParseIncoming reads from bytes and returns the appropriate postgres message type
func (p *Parser) ParseIncoming(buff []byte, msgID uint64) {
	if buff[0] == 0 {
		p.currentRequestMessage.TypeIdentifier = "StartupPacket"
		p.currentRequestMessage.Payload = buff
		p.flushRequest()

		return
	}

	buffLen := uint32(len(buff))

	for buffLen > 0 {
		if p.remainingRequestBytes > 0 {
			rem := p.remainingRequestBytes

			if buffLen < p.remainingRequestBytes {
				p.currentRequestMessage.Payload = append(p.currentRequestMessage.Payload, buff...)
				p.remainingRequestBytes = rem - buffLen

				return
			}

			p.currentRequestMessage.Payload = append(p.currentRequestMessage.Payload, buff[:rem]...)
			p.flushRequest()

			buff = buff[rem+1:]
		} else {
			charTag := rune(buff[0])
			length := binary.BigEndian.Uint32(buff[1:5])
			p.currentRequestMessage.TypeIdentifier = IncomingTypes[charTag]

			if len(buff) < int(length) {
				p.currentRequestMessage.Payload = append(p.currentRequestMessage.Payload, buff[5:]...)
				p.remainingRequestBytes = length - uint32(len(buff))
				return
			}

			p.currentRequestMessage.Payload = append(p.currentRequestMessage.Payload, buff[5:length+1]...)
			p.flushRequest()

			buff = buff[length+1:]
		}

		buffLen = uint32(len(buff))
	}

	return
}

func (p *Parser) flushRequest() {
	p.Outgoing <- *p.currentRequestMessage
	p.currentRequestMessage = &PostgresMessage{Outgoing: false}
	p.remainingRequestBytes = 0
}

// ParseOutgoing reads from bytes and returns the appropriate postgres message type
func (p *Parser) ParseOutgoing(buff []byte, msgID uint64) {
	buffLen := uint32(len(buff))

	for buffLen > 0 {
		if p.remainingResponseBytes > 0 {
			rem := p.remainingResponseBytes

			if buffLen < p.remainingResponseBytes {
				p.currentResponseMessage.Payload = append(p.currentResponseMessage.Payload, buff...)
				p.remainingResponseBytes = rem - buffLen

				return
			}

			p.currentResponseMessage.Payload = append(p.currentResponseMessage.Payload, buff[:rem+1]...)
			p.flushResponse()

			buff = buff[rem+1:]
		} else {
			p.currentResponseMessage.TypeRune = rune(buff[0])
			p.currentResponseMessage.TypeIdentifier = OutoingType[p.currentResponseMessage.TypeRune]

			if buffLen == 1 {
				p.flushResponse()
				return
			}

			length := binary.BigEndian.Uint32(buff[1:5])

			if length == 0 {
				p.flushResponse()
				return
			}

			if buffLen < length {
				p.currentResponseMessage.Payload = append(p.currentResponseMessage.Payload, buff[5:]...)
				p.remainingResponseBytes = length - (buffLen)

				return
			}

			p.currentResponseMessage.Payload = append(p.currentResponseMessage.Payload, buff[5:length+1]...)
			p.flushResponse()

			buff = buff[length+1:]
		}

		buffLen = uint32(len(buff))
	}

	return
}

func (p *Parser) flushResponse() {
	p.Outgoing <- *p.currentResponseMessage
	p.currentResponseMessage = &PostgresMessage{Outgoing: true}
	p.remainingResponseBytes = 0
}
