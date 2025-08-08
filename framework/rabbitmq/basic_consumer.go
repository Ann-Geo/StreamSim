package rabbitmq

import (
	"dstream-sim/constants"
	"dstream-sim/helpers"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
tunables
	//reception
		Consumer Ack

	//consumer initialization
		PrefetchCount     *PrefetchCount

experiment
	//initialize
		Role

	//reception
		Duration


workload
	// receiving charas
		NumConsumers            uint64  `json:"NumConsumers"`
		ConsumerParallelism     *string `json:"ConsumerParallelism"`     // dedicatedMPI, nonMPI
		ConsPayloadDistribution *string `json:"ConsPayloadDistribution"` // roundrobin, random

*/

type BasicConsumer struct {
	*Client
	*BasicConsumeArgs
	ConsTag            string
	ConsQueue          Queue
	ConsRcvdMsgs       <-chan amqp.Delivery
	ConsRawMessage     amqp.Delivery
	ConsPrefetchCount  int
	ConsPrefetchSize   int
	ConsGlobalPrefetch bool //if true applies to to all existing and future
	//consumers on all channels on the same connection.
	//if false the Channel.Qos
	//settings will apply to all existing and future consumers on this channel.
}

type BasicConsumeArgs struct {
	ConsAutoAck   bool
	ConsExclusive bool
	ConsNoLocal   bool
	ConsNoWait    bool
	ConsArgs      amqp.Table
}

func NewBasicConsumer(client *Client) *BasicConsumer {
	return &BasicConsumer{
		Client: client,
	}
}

func (b *BasicConsumer) BasicConsume() error {
	var err error
	b.ConsRcvdMsgs, err = b.Ch.Consume(
		b.ConsQueue.QName,                // queue
		b.ConsTag,                        // consumer
		b.BasicConsumeArgs.ConsAutoAck,   // auto-ack
		b.BasicConsumeArgs.ConsExclusive, // exclusive
		b.BasicConsumeArgs.ConsNoLocal,   // no-local
		b.BasicConsumeArgs.ConsNoWait,    // no-wait
		b.BasicConsumeArgs.ConsArgs,      // args
	)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to start consumption: %v\n", err)
		return err
	}
	return nil
}

// set per channel QoS. Need to prepare the count, size and global
// before calling this
func (b *BasicConsumer) SetChannelQoS() error {
	err := b.Ch.Qos(
		b.ConsPrefetchCount,  // prefetch count
		b.ConsPrefetchSize,   // prefetch size
		b.ConsGlobalPrefetch, // global
	)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to apply Qos prefetch settings: %v\n", err)
		return err

	}
	return nil
}

// find queue index for exp consumer and exp producer to publish
func (b *BasicConsumer) FindQueueIndex(qAssignQueue string, allQueues AllQueues) (*int64, error) {

	var err error
	var ok bool
	b.ConsQueue = allQueues.Queues[qAssignQueue]

	b.ConsPrefetchCount = constants.Q_ASSIGN_Q_CONS_PREFETCH_COUNT
	b.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	b.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH

	b.BasicConsumeArgs = &BasicConsumeArgs{}
	b.ConsAutoAck = true
	b.ConsExclusive = false
	b.ConsNoLocal = false
	b.ConsNoWait = false
	b.ConsArgs = nil

	err = b.SetChannelQoS()
	if err != nil {
		return nil, err
	}
	//start consumption
	err = b.BasicConsume()
	if err != nil {
		return nil, err
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"FindQueueIndex: waiting on queue assingment...\n")
	helpers.DebugLogger.Log(helpers.DEBUG,
		"FindQueueIndex: waiting to consume from %v\n", b.ConsQueue.QName)

	b.ConsRawMessage, ok = <-b.ConsRcvdMsgs

	if !ok {
		err = fmt.Errorf("channel closed before receiving message")
		helpers.ErrorLogger.Log(helpers.ERROR,
			"FindQueueIndex did not receive queue assignment, %v\n", err)
		return nil, err
	}
	//b.ConsRawMessage.Ack(false)

	consQIndex, err := helpers.BytesToInt64LE(b.ConsRawMessage.Body)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "FindQueueIndex: cons queue index retrieval failed: %v\n", err)
		return nil, err
	}
	return &consQIndex, nil
}

func (b *BasicConsumer) ConsShutdown() error {
	// will close() the deliveries channel
	if err := b.Ch.Cancel(b.ConsTag, true); err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "ConsShutdown: Consumer cancel failed: %v\n", err)
		return err
	}

	if err := b.Conn.Close(); err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "ConsShutdown: AMQP connection close error: %v\n", err)
		return err
	}
	return nil
}
