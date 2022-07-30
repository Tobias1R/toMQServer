package main

const (

	// Publish Message
	MsgPublishTcpReq = 1001
	MsgPublishTcpAck = 1002
	// Unacknowledge Message
	MsgNAckTcpReq = 1003
	MsgNAckTcpAck = 1004
	// Acknowledge Message
	MsgAckTcpReq = 1005
	MsgAckTcpAck = 1006
	// Reject Message
	MsgRejectTcpReq = 1007
	MsgRejectTcpAck = 1008
	// Next Message
	MsgNextTcpReq = 1009
	MsgNextTcpAck = 1010
	// Create Channel
	ChannelCreateTcpReq = 1011
	ChannelCreateTcpAck = 1012
	// Join Channel
	ChannelJoinTcpReq = 1013
	ChannelJoinTcpAck = 1014
	// Register Consumer
	ConsumerRegisterTcpReq = 1015
	ConsumerRegisterTcpAck = 1016
	// Register Publisher
	ConsumerPublisherTcpReq = 1017
	ConsumerPublisherTcpAck = 1018
	// distribute
	MsgDistributeTcpReq = 1019
	MsgDistributeTcpAck = 1020
	// LOGIN
	// LOGOFF
)

/*
Message Data will be always encoded in base64


MQ PATTERNS
PUBLISH
REQUEST DATA: #CHANNEL_NAME []BYTE
RESPONSE: [28]byte(MQ_MESSAGE_ID)

NACK
REQUEST DATA: #CHANNEL_NAME [28]BYTE(MQ_MESSAGE_ID)
RESPONSE: [2]byte(OK)

ACK
REQUEST DATA: #CHANNEL_NAME [28]BYTE(MQ_MESSAGE_ID)
RESPONSE: [2]byte(OK)

REJECT
REQUEST DATA: #CHANNEL_NAME [28]BYTE(MQ_MESSAGE_ID)
RESPONSE: [2]byte(OK)

CHANNEL CREATE
REQUEST DATA: #CHANNEL_NAME []BYTE(CREATE)
RESPONSE: [2]byte(OK)

JOIN
REQUEST DATA: #CHANNEL_NAME []BYTE(JOIN)
RESPONSE: [2]byte(OK)

REGISTER CONSUMER
REQUEST DATA: [28]byte
RESPONSE: [2]byte(OK)

*/
