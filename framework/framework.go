package framework

import "dstream-sim/arguments"

type ConnectionConfig interface {
	ParseConnectionConfig(*arguments.SimulatorIns) error
	GetAmqpsUrl() string
	GetTlsEncryption() string
	GetToolkit() string
}

type Tunables interface {
	ParseTunablesConfig(*arguments.SimulatorIns) error
	GetTotalQueues() int64
	GetQueueDurability() string
	GetMessageDurability() string
	GetQueueSizeMessages() int
	GetQueueOverflow() string
	GetPublisherConfirms() (string, int64)
	GetConsumerAcks() (string, string, int64)
	GetPrefetchCount() int
	GetQueueStructure() string
}
