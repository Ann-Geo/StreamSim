package rabbitmq

import (
	"dstream-sim/constants"
	"dstream-sim/helpers"
	"dstream-sim/params"
	"dstream-sim/results"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExpConsumer struct {
	*BasicConsumer
	ConsDuration               uint64 //duration in minutes
	ConsMsgCount               int64  //incrementing, represents current
	ConsMsgCountExpected       int64
	ConsAckMode                string
	ConsManualAckType          string
	ConsManualAckMultipleCount int64
	MsgReceptionTimeChan       chan int64
	ConsShutdownChan           chan struct{}
	*results.Throughput
	ConsQueueIndex *int64

	//concerning dead letter queue
	DlqFirstTS         int64
	DlqLastTS          int64
	DlqMsgCount        int64
	ConsQueueStructure string
	//ConsQueueQuorum    bool
}

func NewExpConsumer(testParams params.TestParams, client *Client) *ExpConsumer {
	var msgCount int64
	if testParams.Workload.DataPackaging.Type == constants.BATCH_SEND {
		msgCount = testParams.Experiment.MsgCount /
			testParams.Workload.DataPackaging.BatchMessageCount
	} else if testParams.Workload.DataPackaging.Type == constants.INDIVIDUAL_SEND {
		msgCount = testParams.Experiment.MsgCount
	}
	return &ExpConsumer{
		BasicConsumer:        NewBasicConsumer(client),
		MsgReceptionTimeChan: make(chan int64, msgCount),
		ConsShutdownChan:     make(chan struct{}),
		Throughput:           &results.Throughput{},
	}
}

func (ec *ExpConsumer) DetermineConsMsgCountExpected(testParams params.TestParams) {
	if testParams.Workload.DataPackaging.Type == constants.BATCH_SEND {
		ec.ConsMsgCountExpected = testParams.Experiment.MsgCount /
			testParams.Workload.DataPackaging.BatchMessageCount
		//(constants.CONS_MSG_DIVIDER * testParams.Workload.NumConsumers * testParams.Workload.DataPackaging.BatchMessageCount)
	} else if testParams.Workload.DataPackaging.Type == constants.INDIVIDUAL_SEND {
		ec.ConsMsgCountExpected = testParams.Experiment.MsgCount /// (constants.CONS_MSG_DIVIDER * testParams.Workload.NumConsumers)
	}
}

func (ec *ExpConsumer) DetermineConsumeQueue(testParams params.TestParams, allQueues AllQueues) error {
	var err error
	qAssignQueue := CreateQueueName(constants.CONS_Q_ASSIGN_Q, constants.CONS_Q_ASSIGN_Q_INDEX)
	ec.ConsQueueIndex, err = ec.FindQueueIndex(qAssignQueue, allQueues)
	if err != nil {
		return err
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"DetermineConsumeQueue: Q assignment received = %v", *ec.ConsQueueIndex)

	var consQName string
	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_PATTERN {
			consQName = CreateQueueName(constants.THROUGHPUT_Q, *ec.ConsQueueIndex)
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN {
			consQName = CreateQueueName(constants.THROUGHPUT_Q, testParams.Experiment.RunnerID)
		}
	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
			consQName = CreateQueueName(constants.LATENCY_REQUEST_Q, *ec.ConsQueueIndex)
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			consQName = CreateQueueName(constants.LATENCY_REQUEST_Q, testParams.Experiment.RunnerID)
		}
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"DetermineConsumeQueue: determined consume queue = %v", consQName)

	ec.ConsQueue = allQueues.Queues[consQName]
	return nil
}

func (ec *ExpConsumer) DeterminePrefetchParams(testParams params.TestParams) {
	ec.BasicConsumer.ConsPrefetchCount = testParams.Tunables.GetPrefetchCount()
	helpers.DebugLogger.Log(helpers.DEBUG, "Prefetch count: %v\n", ec.BasicConsumer.ConsPrefetchCount)
	ec.BasicConsumer.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	ec.BasicConsumer.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH
}

/*
func (ec *ExpConsumer) StartShutdownListener(testParams params.TestParams, allQueues AllQueues) error {
	var err error

	var shutdownSignalConsumeClient *Client

	shutdownSignalConsumeClient, err = NewClient(ec.Connector)
	if err != nil {
		return err
	}
	defer shutdownSignalConsumeClient.Ch.Close()

	bc := NewBasicConsumer(shutdownSignalConsumeClient)
	consQueue := CreateQueueName(constants.CONS_SHUTDOWN_Q, testParams.Experiment.RunnerID)

	bc.ConsQueue = allQueues.Queues[consQueue]

	bc.ConsPrefetchCount = constants.SHUTDOWN_Q_CONS_PREFETCH_COUNT
	bc.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	bc.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH

	bc.BasicConsumeArgs = &BasicConsumeArgs{}
	bc.ConsAutoAck = true
	bc.ConsExclusive = true
	bc.ConsNoLocal = false
	bc.ConsNoWait = false
	bc.ConsArgs = nil

	err = bc.SetChannelQoS()
	if err != nil {
		return err
	}

	//start consumption
	err = bc.BasicConsume()
	if err != nil {
		return err
	}

	for bc.ConsRawMessage = range bc.ConsRcvdMsgs {
		if string(bc.ConsRawMessage.Body) == constants.SHUTDOWN_SIGNAL {
			helpers.DebugLogger.Log(helpers.DEBUG, "Shutdown signal received")
			close(ec.ConsShutdownChan)
			break
		}
	}
	return nil
}
*/

// Instead of checking if/else or switch every time, evaluate it only once
// The loop in ConsumerRunStart just uses the resolved function pointer
// returns a function that accepts an argument of type amqp.Delivery
// TODO: ack error handing
func (ec *ExpConsumer) ConsMessageHandler() func(amqp.Delivery) {
	switch {
	case ec.ConsAckMode == constants.CONS_ACK_AUTO:
		return func(msg amqp.Delivery) {
			helpers.DebugLogger.Log(helpers.DEBUG, "Received (auto-acked)")
			// No manual ack needed
		}

	case ec.ConsAckMode == constants.CONS_ACK_MANUAL &&
		ec.ConsManualAckType == constants.CONS_ACK_MANUAL_SINGLE:
		return func(msg amqp.Delivery) {
			helpers.DebugLogger.Log(helpers.DEBUG, "Received (man-acked-single)")
			msg.Ack(false)
		}

	case ec.ConsAckMode == constants.CONS_ACK_MANUAL &&
		ec.ConsManualAckType == constants.CONS_ACK_MANUAL_MULTIPLE:
		var ackCounter int64
		return func(msg amqp.Delivery) {
			ackCounter++
			helpers.DebugLogger.Log(helpers.DEBUG, "Received (man-acked-multiple)")
			helpers.DebugLogger.Log(helpers.DEBUG, "Ackcounter: %v\n", ackCounter)
			if ackCounter%ec.ConsManualAckMultipleCount == 0 {
				msg.Ack(true) //ack multiple messages
			}
		}

	default:
		return func(msg amqp.Delivery) {
			helpers.ErrorLogger.Log(helpers.ERROR, "Unsupported ack mode")
		}
	}
}

func (ec *ExpConsumer) ReplyPublisher(rp *BasicProducer, testParams params.TestParams, allQueues AllQueues) error {
	var err error

	rp.PubMsgContentType = ec.ConsRawMessage.ContentType
	rp.PubQueue.QName = ec.ConsRawMessage.ReplyTo

	rp.PubMessage = amqp.Publishing{
		DeliveryMode:  rp.PubMsgDurability,
		ContentType:   rp.PubMsgContentType,
		CorrelationId: ec.ConsRawMessage.CorrelationId,
		Body:          ec.ConsRawMessage.Body}

	err = rp.BasicPublish()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"ReplyPublisher failed to publish, %v\n", err)
		return err
	}
	return nil
}

func (ec *ExpConsumer) ConsMsgCountPublisher(allQueues AllQueues) error {
	var err error
	var countPubClient *Client
	//var totalMsgCount int64

	countPubClient, err = NewClient(ec.Connector)
	if err != nil {
		return err
	}
	defer countPubClient.Ch.Close()

	bp := NewBasicProducer(countPubClient)

	bp.PubExchange = constants.EMPTY_EX_NAME
	bp.PubMandatory = false
	bp.PubImmediate = false
	bp.PubMsgDurability = amqp.Transient
	bp.PubMsgContentType = constants.PLAINTEXT_CONTENT
	pubQueue := CreateQueueName(constants.CONS_MSGCOUNT_Q, constants.CONS_MSGCOUNT_Q_INDEX)
	bp.PubQueue = allQueues.Queues[pubQueue]

	//bp.MonitorChannelClosure()

	//msgToBeSend := []byte(fmt.Sprintf("1,%d", ts))
	msgToBeSend := []byte(fmt.Sprintf("%d", ec.ConsMsgCount))
	bp.PubMessage = amqp.Publishing{
		DeliveryMode: bp.PubMsgDurability,
		ContentType:  bp.PubMsgContentType,
		Body:         []byte(msgToBeSend)}

	err = bp.BasicPublish()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"ConsMsgCountPublisher failed to publish, %v\n", err)
		return err
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"ConsMsgCountPublisher: FirstTS, %v\n", ec.FirstTS)
	helpers.DebugLogger.Log(helpers.DEBUG,
		"ConsMsgCountPublisher: LastTS, %v\n", ec.LastTS)
	helpers.DebugLogger.Log(helpers.DEBUG,
		"ConsMsgCountPublisher: totalMsgCount at the consumer, %v\n", ec.ConsMsgCount)

	return nil
}

func (ec *ExpConsumer) PublishThroughput(allQueues AllQueues) error {
	var err error
	bp := NewBasicProducer(ec.Client)
	bp.PubExchange = constants.EMPTY_EX_NAME
	bp.PubMandatory = false
	bp.PubImmediate = false
	bp.PubMsgDurability = amqp.Transient
	bp.PubMsgContentType = constants.PLAINTEXT_CONTENT
	pubQueue := CreateQueueName(constants.RESULT_Q, constants.RESULT_Q_INDEX)
	bp.PubQueue = allQueues.Queues[pubQueue]

	msgToBeSend := []byte(fmt.Sprintf("%f", ec.Throughput.ThroughputValue))
	helpers.DebugLogger.Log(helpers.DEBUG,
		"PulishThroughput: throughput value to be send in float - %v, in bytes - %v\n",
		ec.Throughput.ThroughputValue, msgToBeSend)
	bp.PubMessage = amqp.Publishing{
		DeliveryMode: bp.PubMsgDurability,
		ContentType:  bp.PubMsgContentType,
		Body:         []byte(msgToBeSend)}
	err = bp.BasicPublish()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"PulishThroughput: failed to publish, %v\n", err)
		return err
	}
	return nil
}

/*
func (ec *ExpConsumer) JustMonitorDlq() {
	for {
		q, err := ec.Ch.QueueDeclarePassive(
			"consQ_deadletter_0", // queue name
			false,                // durable
			false,                // delete when unused
			false,                // exclusive
			false,                // no-wait
			nil,                  // arguments
		)
		if err != nil {
			log.Printf("QueueDeclarePassive error: %v\n", err)
		} else {
			fmt.Printf("DLQ Message Count: %d\n",
				q.Messages)
		}
		time.Sleep(2 * time.Second) // Poll every 5 seconds
	}
}
*/

func (ec *ExpConsumer) ConsumeFromDlq(testParams params.TestParams, allQueues AllQueues) error {
	helpers.DebugLogger.Log(helpers.DEBUG, "ConsumeFromDlq: Started..")

	dlqClient, err := NewClient(ec.Connector)
	if err != nil {
		return err
	}
	defer dlqClient.Ch.Close()

	dlqConsumer := NewBasicConsumer(dlqClient)
	dlqQueueName := CreateQueueName(constants.CONS_DEADLETTER_Q, *ec.ConsQueueIndex)
	dlqConsumer.ConsQueue = allQueues.Queues[dlqQueueName]

	dlqConsumer.ConsPrefetchCount = constants.DEADLETTER_Q_CONS_PREFETCH_COUNT
	dlqConsumer.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	dlqConsumer.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH
	err = dlqConsumer.SetChannelQoS()
	if err != nil {
		return err
	}

	// DLQ consumption setup
	dlqConsumer.BasicConsumeArgs = &BasicConsumeArgs{}
	dlqConsumer.ConsAutoAck = true
	dlqConsumer.ConsExclusive = false
	dlqConsumer.ConsNoWait = false
	dlqConsumer.ConsArgs = nil

	err = dlqConsumer.BasicConsume()
	if err != nil {
		return err
	}

	for dlqConsumer.ConsRawMessage = range dlqConsumer.ConsRcvdMsgs {
		ts := time.Now().UnixNano()
		helpers.DebugLogger.Log(helpers.DEBUG, "ConsumerRunStart: Received msg of size: %v on DLQ queue\n", len(dlqConsumer.ConsRawMessage.Body))
		if ec.DlqMsgCount == 0 {
			ec.DlqFirstTS = ts
		}
		ec.DlqLastTS = ts
		ec.DlqMsgCount++
		ec.MsgReceptionTimeChan <- ts
	}
	return nil
}

func (ec *ExpConsumer) ConsumerRunStart(testParams params.TestParams, allQueues AllQueues) error {
	var waitGroups int
	var errChCount int64

	waitGroups = 0
	if ec.ConsQueueStructure == constants.QUORUM_Q &&
		testParams.Tunables.GetQueueOverflow() == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
		waitGroups = 1
		errChCount = 1
	}

	var wg sync.WaitGroup
	wg.Add(waitGroups)
	var err error

	//prefetch params - 3
	ec.DeterminePrefetchParams(testParams)
	err = ec.SetChannelQoS()
	if err != nil {
		return err
	}

	//get acks params
	ec.ConsAckMode, ec.ConsManualAckType, ec.ConsManualAckMultipleCount = testParams.Tunables.GetConsumerAcks()
	//prepare parameters
	if ec.ConsAckMode == constants.CONS_ACK_AUTO {
		//server will automatically take care of delivery acks
		ec.BasicConsumeArgs.ConsAutoAck = true
	} else if ec.ConsAckMode == constants.CONS_ACK_MANUAL {
		//need to manually ack
		ec.BasicConsumeArgs.ConsAutoAck = false
	}
	//true - server assumes this is the sole consumer on the queue
	//false - will distribute messages equally to all consumers
	ec.BasicConsumeArgs.ConsExclusive = false

	//not supported by RabbitMQ
	ec.BasicConsumeArgs.ConsNoLocal = false

	//true - Client does not wait for the server’s confirmation.
	//It assumes consume was successful and starts reading messages.
	//If the consume fails, RabbitMQ closes the channel with a protocol exception.
	//false - Client waits for server's response (a basic.consume-ok).
	//If the request is invalid (e.g., queue doesn’t exist),
	//it gets an error and can react.
	ec.BasicConsumeArgs.ConsNoWait = true

	ec.BasicConsumeArgs.ConsArgs = nil

	ec.BasicConsumer.ConsTag = fmt.Sprintf("%d", testParams.Experiment.RunnerID)

	//start consumption
	err = ec.BasicConsume()
	if err != nil {
		return err
	}

	errCh := make(chan error, errChCount)
	//start shutdownlistener
	/*
		go func() {
			err = ec.StartShutdownListener(testParams, allQueues)
			errCh <- err
		}()
	*/

	//start message count publisher
	/*
		go func() {
			defer wg.Done()
			err = ec.ConsMsgCountPublisher(allQueues)
			errCh <- err
		}()
	*/

	//start dlq consumer only if quorum is and dead letter is enabled
	if ec.ConsQueueStructure == constants.QUORUM_Q &&
		testParams.Tunables.GetQueueOverflow() == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
		go func() {
			defer wg.Done()
			err = ec.ConsumeFromDlq(testParams, allQueues)
			errCh <- err
		}()
	}

	//go ec.JustMonitorDlq()
	//intialize reply publisher
	var replyPubClient *Client
	replyPubClient, err = NewClient(ec.Connector)
	if err != nil {
		return err
	}
	defer replyPubClient.Ch.Close()

	rp := NewBasicProducer(replyPubClient)

	rp.PubMandatory = false
	rp.PubImmediate = false
	//reply should be of same durability as of request?
	//bp.DetermineMsgDurability(testParams)
	rp.PubMsgDurability = amqp.Transient
	rp.PubQueue = Queue{}

	if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
		rp.PubExchange = CreateExName(constants.PROD_REPLY_EX, constants.PROD_REPLY_EX_INDEX)
	} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
		rp.PubExchange = constants.EMPTY_EX_NAME
	}
	//consumer reception idle time
	timeoutDuration := 30 * time.Second
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()
	//reception
	consMsgHandler := ec.ConsMessageHandler()
messageloop:
	for {
		select {
		case ec.ConsRawMessage, _ = <-ec.ConsRcvdMsgs:
			ts := time.Now().UnixNano()
			helpers.DebugLogger.Log(helpers.DEBUG, "ConsumerRunStart: Received %v msg of size: %v on source queue %v\n", ec.ConsMsgCount, len(ec.ConsRawMessage.Body), ec.ConsQueue.QName)
			consMsgHandler(ec.ConsRawMessage)
			if ec.ConsMsgCount == 0 {
				ec.FirstTS = ts
			}

			ec.ConsMsgCount = ec.ConsMsgCount + 1
			//if ec.ConsMsgCount == ec.ConsMsgCountExpected {

			ec.LastTS = ts
			if !timeout.Stop() {
				<-timeout.C
			}
			timeout.Reset(timeoutDuration)
			//ec.ConsRawMessage.Ack(false)
			//_ = ec.Ch.Cancel(ec.BasicConsumer.ConsTag, true)

			//for range ec.ConsRcvdMsgs {
			//}
			if testParams.Experiment.TestType == constants.LATENCY_TEST {
				err = ec.ReplyPublisher(rp, testParams, allQueues)
				if err != nil {
					return err
				}
			}
			if ec.ConsMsgCount == ec.ConsMsgCountExpected {
				helpers.DebugLogger.Log(helpers.DEBUG, "ConsumerRunStart: Reached expected msg count. Breaking.")
				break messageloop
			}
		case <-timeout.C:
			break messageloop
			//}
		}
	}
	//for range ec.ConsRcvdMsgs {
	//}
	// publish
	err = ec.ConsMsgCountPublisher(allQueues)
	if err != nil {
		return err
	}

	//compare timestamps if quorum is true and deadletter is enabled
	if ec.ConsQueueStructure == constants.QUORUM_Q &&
		testParams.Tunables.GetQueueOverflow() == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
		if ec.DlqLastTS > ec.LastTS {
			ec.LastTS = ec.DlqLastTS
		}
		if ec.DlqFirstTS > 0 && (ec.FirstTS == 0 || ec.DlqFirstTS < ec.FirstTS) {
			ec.FirstTS = ec.DlqFirstTS
		}
	}

	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		ec.CalculateThroughPut(ec.ConsMsgCount)
		helpers.InfoLogger.Log(helpers.INFO,
			"ConsumerRunStart: Throughput, %v\n", ec.Throughput.ThroughputValue)
		//throughput publish
		ec.PublishThroughput(allQueues)
	}
	/*
		for {
			select {
			case <-ec.ConsShutdownChan:
				helpers.DebugLogger.Log(helpers.DEBUG, "ConsumerRunStart: Graceful Shutdown of consumer")
				wg.Wait()
				//compare timestamps if quorum is true and deadletter is enabled
				if ec.ConsQueueStructure == constants.QUORUM_Q &&
					testParams.Tunables.GetQueueOverflow() == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
					if ec.DlqLastTS > ec.LastTS {
						ec.LastTS = ec.DlqLastTS
					}
					if ec.DlqFirstTS > 0 && (ec.FirstTS == 0 || ec.DlqFirstTS < ec.FirstTS) {
						ec.FirstTS = ec.DlqFirstTS
					}
				}
				ec.CalculateThroughPut(ec.ConsMsgCount)
				helpers.InfoLogger.Log(helpers.INFO,
					"ConsumerRunStart: Throughput, %v\n", ec.Throughput.ThroughputValue)
				//throughput publish
				ec.PublishThroughput(allQueues)
				return nil
			case err := <-errCh:
				if err != nil {
					helpers.ErrorLogger.Log(helpers.ERROR, "ConsumerRunStart: Routines returned error: %v", err)
					return err
				}
			}
		}
	*/
	err = ec.ConsShutdown()
	if err != nil {
		return err
	}
	return nil
}

func (ec *ExpConsumer) RunConsumer(testParams params.TestParams, allQueues AllQueues) error {
	var err error

	helpers.DebugLogger.Log(helpers.DEBUG,
		"RunConsumer: Starting consumer ...\n")
	//determine queue
	err = ec.DetermineConsumeQueue(testParams, allQueues)
	if err != nil {
		return err
	}

	ec.ConsQueueStructure = testParams.Tunables.GetQueueStructure()
	//ec.ConsQueueQuorum = IsQuorumQueueRequired(testParams)
	ec.DetermineConsMsgCountExpected(testParams)

	ec.MonitorChannelClosure()

	err = ec.ConsumerRunStart(testParams, allQueues)
	if err != nil {
		return err
	}

	return nil

}
