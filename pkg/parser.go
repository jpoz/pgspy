package pgspy

import (
	"encoding/binary"
)

const (
	// BindIncoming Incoming Message
	BindIncoming = "Bind"
	// CloseIncoming Incoming Message
	CloseIncoming = "Close"
	// CopyDataIncoming Incoming Message
	CopyDataIncoming = "CopyData"
	// CopyDoneIncoming Incoming Message
	CopyDoneIncoming = "CopyDone"
	// CopyFailIncoming Incoming Message
	CopyFailIncoming = "CopyFail"
	// DescribeIncoming Incoming Message
	DescribeIncoming = "Describe"
	// ExecuteIncoming Incoming Message
	ExecuteIncoming = "Execute"
	// FlushIncoming Incoming Message
	FlushIncoming = "Flush"
	// FunctionCallIncoming Incoming Message
	FunctionCallIncoming = "FunctionCall"
	// ParseIncoming Incoming Message
	ParseIncoming = "Parse"
	// PasswordMessageIncoming Incoming Message
	PasswordMessageIncoming = "PasswordMessage"
	// QueryIncoming Incoming Message
	QueryIncoming = "Query"
	// SyncIncoming Incoming Message
	SyncIncoming = "Sync"
	// TerminateIncoming Incoming Message
	TerminateIncoming = "Terminate"

	// AuthenticationOutgoing Outgoing Message
	AuthenticationOutgoing = "Authentication"
	// BackendKeyDataOutgoing Outgoing Message
	BackendKeyDataOutgoing = "BackendKeyData"
	// BindCompleteOutgoing Outgoing Message
	BindCompleteOutgoing = "BindComplete"
	// CloseCompleteOutgoing Outgoing Message
	CloseCompleteOutgoing = "CloseComplete"
	// CommandCompleteOutgoing Outgoing Message
	CommandCompleteOutgoing = "CommandComplete"
	// CopyDataOutgoing Outgoing Message
	CopyDataOutgoing = "CopyData"
	// CopyDoneOutgoing Outgoing Message
	CopyDoneOutgoing = "CopyDone"
	// CopyInResponseOutgoing Outgoing Message
	CopyInResponseOutgoing = "CopyInResponse"
	// CopyOutResponseOutgoing Outgoing Message
	CopyOutResponseOutgoing = "CopyOutResponse"
	// CopyBothResponseOutgoing Outgoing Message
	CopyBothResponseOutgoing = "CopyBothResponse"
	// DataRowOutgoing Outgoing Message
	DataRowOutgoing = "DataRow"
	// EmptyQueryResponseOutgoing Outgoing Message
	EmptyQueryResponseOutgoing = "EmptyQueryResponse"
	// ErrorResponseOutgoing Outgoing Message
	ErrorResponseOutgoing = "ErrorResponse"
	// FunctionCallResponseOutgoing Outgoing Message
	FunctionCallResponseOutgoing = "FunctionCallResponse"
	// NegotiateProtocolVersionOutgoing Outgoing Message
	NegotiateProtocolVersionOutgoing = "NegotiateProtocolVersion"
	// NoDataOutgoing Outgoing Message
	NoDataOutgoing = "NoData"
	// NoticeResponseOutgoing Outgoing Message
	NoticeResponseOutgoing = "NoticeResponse"
	// NotificationResponseOutgoing Outgoing Message
	NotificationResponseOutgoing = "NotificationResponse"
	// ParameterDescriptionOutgoing Outgoing Message
	ParameterDescriptionOutgoing = "ParameterDescription"
	// ParameterStatusOutgoing Outgoing Message
	ParameterStatusOutgoing = "ParameterStatus"
	// ParseCompleteOutgoing Outgoing Message
	ParseCompleteOutgoing = "ParseComplete"
	// PortalSuspendedOutgoing Outgoing Message
	PortalSuspendedOutgoing = "PortalSuspended"
	// ReadyForQueryOutgoing Outgoing Message
	ReadyForQueryOutgoing = "ReadyForQuery"
	// RowDescriptionOutgoing Outgoing Message
	RowDescriptionOutgoing = "RowDescription"
)

// IncomingTypes is a map from the charTag to the message type
var IncomingTypes = map[rune]string{
	'B': BindIncoming,
	'C': CloseIncoming,
	'd': CopyDataIncoming,
	'c': CopyDoneIncoming,
	'f': CopyFailIncoming,
	'D': DescribeIncoming,
	'E': ExecuteIncoming,
	'H': FlushIncoming,
	'F': FunctionCallIncoming,
	'P': ParseIncoming,
	'p': PasswordMessageIncoming,
	'Q': QueryIncoming,
	'S': SyncIncoming,
	'X': TerminateIncoming,
}

// OutgoingType is a map from the charTag to the message type
var OutgoingType = map[rune]string{
	'R': AuthenticationOutgoing,
	'K': BackendKeyDataOutgoing,
	'2': BindCompleteOutgoing,
	'3': CloseCompleteOutgoing,
	'C': CommandCompleteOutgoing,
	'd': CopyDataOutgoing,
	'c': CopyDoneOutgoing,
	'G': CopyInResponseOutgoing,
	'H': CopyOutResponseOutgoing,
	'W': CopyBothResponseOutgoing,
	'D': DataRowOutgoing,
	'I': EmptyQueryResponseOutgoing,
	'E': ErrorResponseOutgoing,
	'V': FunctionCallResponseOutgoing,
	'v': NegotiateProtocolVersionOutgoing,
	'n': NoDataOutgoing,
	'N': NoticeResponseOutgoing,
	'A': NotificationResponseOutgoing,
	't': ParameterDescriptionOutgoing,
	'S': ParameterStatusOutgoing,
	'1': ParseCompleteOutgoing,
	's': PortalSuspendedOutgoing,
	'Z': ReadyForQueryOutgoing,
	'T': RowDescriptionOutgoing,
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

			p.currentRequestMessage.Payload = append(p.currentRequestMessage.Payload, buff[:rem+1]...)
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
			p.currentResponseMessage.TypeIdentifier = OutgoingType[p.currentResponseMessage.TypeRune]

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
