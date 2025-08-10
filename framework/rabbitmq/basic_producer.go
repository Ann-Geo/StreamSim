package rabbitmq

import (
	"StreamSim/constants"
	"StreamSim/helpers"
	"StreamSim/params"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
tunables
	//sending
		PubConfirms
			batch size
		Message durability

experiment
	//initialize
		Role

	//sending
		Duration


workload
	//sending
		DataRate            uint64
		DataPackaging       *DataPackagingConfig
		Variability         *string - not now
		NumProducers 		uint64
		ProducerParallelism *string
*/

type BasicProducer struct {
	*Client
	PubQueue    Queue
	PubExchange string
	//CorrIDStore *MessageStore
	PubMandatory      bool
	PubImmediate      bool
	PubMsgDurability  uint8
	PubMsgContentType string
	PubMessage        amqp.Publishing
	PubReturns        chan amqp.Return
}

func NewBasicProducer(client *Client) *BasicProducer {
	return &BasicProducer{
		Client: client,
		//CorrIDStore: NewMessageStore(),
	}
}

// prepare PubMessage with data before calling this
func (p *BasicProducer) BasicPublish() error {
	var err error
	//TODO: Is context actually required?
	//ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	//defer cancel()

	select {
	case <-p.CPChClose:
		helpers.DebugLogger.Log(helpers.DEBUG, "BasicPublish: channel closure\n")
	default:
		err := p.Ch.Publish(
			p.PubExchange,    // Exchange
			p.PubQueue.QName, // Routing key
			p.PubMandatory,   // Mandatory
			p.PubImmediate,   // Immediate
			p.PubMessage,
		)
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "BasicPublish failed to publish a message: %v\n", err)
			return err
		}
		helpers.DebugLogger.Log(helpers.DEBUG,
			"BasicPublish: published message of size %v to %v\n", len(p.PubMessage.Body), p.PubQueue.QName)
		return nil
	}
	return err
}

func (p *BasicProducer) BasicPublishWithRetry() error {
	var err error

	for attempt := 1; attempt <= constants.PROD_RETRIES; attempt++ {
		select {
		case <-p.CPChClose:
			helpers.DebugLogger.Log(helpers.DEBUG, "BasicPublishWithRetry: channel closure detected\n")
		default:
			err = p.Ch.Publish(
				p.PubExchange,    // Exchange
				p.PubQueue.QName, // Routing key
				p.PubMandatory,   // Mandatory
				p.PubImmediate,   // Immediate
				p.PubMessage,
			)

			if err == nil {
				helpers.DebugLogger.Log(helpers.DEBUG,
					"BasicPublishWithRetry: successfully published message of size %v to %v\n",
					len(p.PubMessage.Body), p.PubQueue.QName)
				return nil
			}

			helpers.ErrorLogger.Log(helpers.ERROR,
				"BasicPublishWithRetry: attempt %d failed to publish message: %v\n", attempt, err)

			if attempt < constants.PROD_RETRIES {
				backoff := constants.RETRY_INTERVAL * (1 << (attempt - 1)) // Exponential backoff
				helpers.DebugLogger.Log(helpers.DEBUG,
					"BasicPublishWithRetry: backing off for %v before retrying...\n", backoff)
				time.Sleep(backoff)
			}
		}
	}

	return fmt.Errorf("publish failed after %d attempts, last error: %v", constants.PROD_RETRIES, err)
}

// cleaner way to sync ack each individual message - not used yet
func (p *BasicProducer) DeferredConfirmPublish() error {
	dconf, err := p.Ch.PublishWithDeferredConfirm(
		"",               // Exchange
		p.PubQueue.QName, // Routing key
		p.PubMandatory,   // Mandatory
		p.PubImmediate,   // Immediate
		p.PubMessage,
	)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to publish a message: %v\n", err)
		return err
	}

	if dconf.Wait() {
		helpers.DebugLogger.Log(helpers.DEBUG, "DeferredConfirm: individual sync ack received")
	}
	helpers.DebugLogger.Log(helpers.DEBUG, "DeferredConfirm: individual sync ack NOT received (NACK)")
	return nil
}

func (p *BasicProducer) NotifyPublishReturns() {
	p.PubReturns = p.Ch.NotifyReturn(make(chan amqp.Return, 1000))
}

func (p *BasicProducer) DetermineMsgDurability(testParams params.TestParams) {
	msgDurability := testParams.Tunables.GetMessageDurability()
	if msgDurability == constants.PERSISTENT {
		p.PubMsgDurability = amqp.Persistent
	} else if msgDurability == constants.NONE {
		p.PubMsgDurability = amqp.Transient
	}
}
