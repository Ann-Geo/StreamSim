package rabbitmq

import (
	"dstream-sim/constants"
	"dstream-sim/helpers"
	"dstream-sim/params"
	"dstream-sim/results"
	"dstream-sim/store"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExpProducer struct {
	*BasicProducer
	PubDuration          int64 //duration in minutes
	PubMsgCount          int64 //always total
	PubConfirmMode       string
	PubConfirmBatchSize  int64
	PubQueueStructure    string
	PubQueueOverflowMode string
	PublishAllowed       atomic.Bool
	//PubQueueQuorum       bool
	ReplyQueue    Queue
	CorrIDStore   *store.MessageStore
	ReplyMsgCount int64
	*results.Rtt
	ProdThreadCount    int
	ExpectedReplyCount int64
}

func NewExpProducer(client *Client) *ExpProducer {
	return &ExpProducer{
		BasicProducer: NewBasicProducer(client),
		CorrIDStore:   store.NewMessageStore(),
		Rtt:           results.NewRtt(),
	}
}

func (ep *ExpProducer) WaitForFlow() {
	for !ep.PublishAllowed.Load() {
		time.Sleep(time.Millisecond * 10) // light back-off
	}
}

func (ep *ExpProducer) DetermineMsgPackaging(testParams params.TestParams, msg []byte) []byte {
	var MsgToBeSend []byte
	if testParams.Workload.DataPackaging.Type == constants.INDIVIDUAL_SEND {
		MsgToBeSend = append(MsgToBeSend, msg...)

	} else if testParams.Workload.DataPackaging.Type == constants.BATCH_SEND {
		for range testParams.Workload.DataPackaging.BatchMessageCount {
			MsgToBeSend = append(MsgToBeSend, msg...)
		}
	}
	return MsgToBeSend
}

func (ep *ExpProducer) DetermineMsgContentType(testParams params.TestParams) {
	contentType := testParams.Workload.PayloadFormat
	if contentType == constants.PAYLOAD_PLAINTEXT {
		ep.BasicProducer.PubMsgContentType = constants.PLAINTEXT_CONTENT
	} else if contentType == constants.PAYLOAD_JSON {
		ep.BasicProducer.PubMsgContentType = constants.JSON_CONTENT
	} else if contentType == constants.PAYLOAD_HDF5 {
		ep.BasicProducer.PubMsgContentType = constants.HDF5_CONTENT
	} else if contentType == constants.PAYLOAD_BINARY {
		ep.BasicProducer.PubMsgContentType = constants.BINARY_CONTENT
	}
}

func (ep *ExpProducer) DeterminePubMsgCount(testParams params.TestParams) {
	if testParams.Workload.DataPackaging.Type == constants.BATCH_SEND {
		ep.PubMsgCount = testParams.Experiment.MsgCount /
			testParams.Workload.DataPackaging.BatchMessageCount
	} else if testParams.Workload.DataPackaging.Type == constants.INDIVIDUAL_SEND {
		ep.PubMsgCount = testParams.Experiment.MsgCount
	}
}

func (ep *ExpProducer) DecideWhichBasicPublish(testParams params.TestParams) error {
	var err error

	var corrId string
	if testParams.Experiment.TestType == constants.LATENCY_TEST {
		corrId = helpers.RandomString(constants.CORRELATION_ID_LENGTH)
		ep.BasicProducer.PubMessage.CorrelationId = corrId
		ep.BasicProducer.PubMessage.ReplyTo = ep.ReplyQueue.QName
	}

	if ep.PubQueueStructure == constants.CLASSIC_Q {
		if ep.PubQueueOverflowMode == constants.Q_OVERFLOW_TYPE_DROP_HEAD {
			err = ep.BasicPublish()
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR,
					"DecideWhichBasicPublish: basic publish failed, %v\n", err)
			}
		} else if ep.PubQueueOverflowMode == constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH {
			//TODO: analyze return and implement republish
			err = ep.BasicPublishWithRetry()
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR,
					"DecideWhichBasicPublish: retrying publish failed, %v\n", err)
			}
		}
	} else if ep.PubQueueStructure == constants.QUORUM_Q {
		if ep.PubQueueOverflowMode == constants.Q_OVERFLOW_TYPE_DROP_HEAD {
			err = ep.BasicPublish()
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR,
					"DecideWhichBasicPublish: basic publish failed, %v\n", err)
			}
		} else if ep.PubQueueOverflowMode == constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH {
			//TODO: analyze return and implement republish
			err = ep.BasicPublishWithRetry()
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR,
					"DecideWhichBasicPublish: retrying publish failed, %v\n", err)
			}
		} else if ep.PubQueueOverflowMode == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
			err = ep.BasicPublish()
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR,
					"DecideWhichBasicPublish: basic publish failed, %v\n", err)
			}
		}
	} else if ep.PubQueueStructure == constants.STREAM_Q {
		//TODO: analyze return and implement republish
		err = ep.BasicPublishWithRetry()
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR,
				"DecideWhichBasicPublish: retrying publish failed, %v\n", err)
		}
	}
	if testParams.Experiment.TestType == constants.LATENCY_TEST {
		ep.CorrIDStore.Store(corrId, time.Now())
	}
	return err
}

func (ep *ExpProducer) PublishMessages(publishFunc func() error, testParams params.TestParams) (int, error) {
	counter := 0
	var i int64
	var pubMsgCount int64

	if testParams.Experiment.TestMode == constants.DURATION_TEST_MODE {
		ep.PubDuration = testParams.Experiment.Duration
		if ep.PubDuration > 0 {
			end := time.Now().Add(time.Duration(testParams.Experiment.Duration) * time.Second)
			for time.Now().Before(end) {
				//ep.WaitForFlow()
				if err := publishFunc(); err != nil {
					return counter, err
				}
				counter++
			}
		}
	} else if testParams.Experiment.TestMode == constants.COUNT_TEST_MODE {
		if ep.PubConfirmMode == constants.PUB_BATCH_SYNC {
			pubMsgCount = ep.PubConfirmBatchSize
		} else {
			pubMsgCount = ep.PubMsgCount
		}
		helpers.DebugLogger.Log(helpers.DEBUG, "Pub ack batch size: %v\n", pubMsgCount)
		for i = 0; i < pubMsgCount; i++ {
			//for {
			//ep.WaitForFlow()
			if err := publishFunc(); err != nil {
				return counter, err
			}
			counter++
			helpers.DebugLogger.Log(helpers.DEBUG, "Published %v messages\n", counter)
		}
	}
	return counter, nil
}

func (ep *ExpProducer) PublishWithNoAck(testParams params.TestParams) error {
	var pubErr error
	helpers.DebugLogger.Log(helpers.DEBUG, "Publishing without publisher acks ...")

	_, pubErr = ep.PublishMessages(func() error {
		return ep.DecideWhichBasicPublish(testParams)
	}, testParams)
	if pubErr != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"PublishWithNoAck failed to publish, %v\n", pubErr)
	}
	return pubErr
}

func (ep *ExpProducer) PublishWithIndividualAsyncAck(testParams params.TestParams) error {
	var pubErr error
	helpers.DebugLogger.Log(helpers.DEBUG, "Publishing with publisher individual async ack ...")
	ep.BasicProducer.Ch.Confirm(false)

	//giving 10K - channel will not block if publisher is too fast
	//too low will block or drop confirms
	//10K is good for <10K messages
	//TODO:change the confirm count from MsgCount to a large number after
	//getting an idea of messages will be sent in a fixed duration
	//(1.while_testing.md)
	confirms := ep.BasicProducer.Ch.NotifyPublish(make(chan amqp.Confirmation,
		ep.PubMsgCount))

	var wg sync.WaitGroup
	//TODO: int casting need to remove later
	wg.Add(int(ep.PubMsgCount))

	//c.DeliveryTag is a A unique, monotonically increasing
	//number assigned by the broker to each published message on a per-channel basis.
	//It starts at 1 and helps to track which message was acknowledged.
	//only valid within the current channel
	go func() {
		for c := range confirms {
			if c.Ack {
				helpers.DebugLogger.Log(helpers.DEBUG, "Individual aynchronous ack delivery tag - %d", c.DeliveryTag)
			} else {
				helpers.DebugLogger.Log(helpers.DEBUG, "Individual aynchronous NO ack delivery tag - %d", c.DeliveryTag)
			}
			wg.Done()
		}

	}()

	_, pubErr = ep.PublishMessages(func() error {
		return ep.DecideWhichBasicPublish(testParams)
	}, testParams)
	if pubErr != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"PublishWithIndividualAsyncAck failed to publish, %v\n", pubErr)
		return pubErr
	}

	wg.Wait()

	return nil
}

func (ep *ExpProducer) PublishWithIndividualSyncAck(testParams params.TestParams) error {
	var pubErr error
	helpers.DebugLogger.Log(helpers.DEBUG, "Publishing with publisher individual sync ack ...")
	ep.BasicProducer.Ch.Confirm(false)
	confirms := ep.BasicProducer.Ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	/*
		//TODO: Returns not receiving anything - added to check if queue overflow
		//can be captured.
			go func() {
				helpers.DebugLogger.Log(helpers.DEBUG, "Waiting for returns...")
				for ret := range ep.BasicProducer.PubReturns {
					helpers.WarnLogger.Log(helpers.WARN,
						"Returned message in no-ack: Reason=%s Body=%s",
						ret.ReplyText, string(ret.Body))
				}
			}()
	*/

	_, pubErr = ep.PublishMessages(func() error {
		if err := ep.DecideWhichBasicPublish(testParams); err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR,
				"PublishWithIndividualSyncAck failed to publish, %v\n", err)
			return err
		}
		confirm, ok := <-confirms
		if !ok {
			err := fmt.Errorf("confirmation channel closed unexpectedly")
			helpers.ErrorLogger.Log(helpers.ERROR,
				"PublishWithIndividualSyncAck failed to publish, %v\n", err)
			return err
		}
		if confirm.Ack {
			helpers.DebugLogger.Log(helpers.DEBUG, "Individual sync ack received")
		} else {
			helpers.DebugLogger.Log(helpers.DEBUG, "Individual sync ack NOT received")
		}
		return nil
	}, testParams)

	return pubErr
}

/*//using DeferredConfirmPublish() - cleaner way
func (ep *ExpProducer) PublishWithIndividualSyncAck(testParams params.TestParams) error {
	var pubErr error
	helpers.DebugLogger.Log(helpers.DEBUG, "Publishing with publisher individual sync ack using DeferredConfirmPublish...")
	ep.BasicProducer.Ch.Confirm(false)

	ep.PublishMessages(func() {
		if err := ep.DeferredConfirmPublish(); err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR,
				"PublishWithIndividualSyncAck failed to publish: %v", err)
			pubErr = err
		}
	}, testParams)

	return pubErr
}
*/

func (ep *ExpProducer) PublishWithBatchSyncAck(testParams params.TestParams) error {
	var pubErr error
	var batchIdx int64
	var numBatches int64
	var i int64
	helpers.DebugLogger.Log(helpers.DEBUG, "Publishing with publisher batch sync ack ...")
	ep.BasicProducer.Ch.Confirm(false)

	numBatches = ep.PubMsgCount / ep.PubConfirmBatchSize
	helpers.DebugLogger.Log(helpers.DEBUG,
		"PublishWithBatchSyncAck: msg count: %v\n", ep.PubMsgCount)
	helpers.DebugLogger.Log(helpers.DEBUG,
		"PublishWithBatchSyncAck: batch size: %v\n", ep.PubConfirmBatchSize)
	helpers.DebugLogger.Log(helpers.DEBUG,
		"PublishWithBatchSyncAck: num batches: %v\n", numBatches)

	confirms := ep.BasicProducer.Ch.NotifyPublish(make(chan amqp.Confirmation,
		ep.PubConfirmBatchSize))

	for batchIdx = 0; batchIdx < numBatches; batchIdx++ {

		_, pubErr = ep.PublishMessages(func() error {
			return ep.DecideWhichBasicPublish(testParams)
		}, testParams)
		if pubErr != nil {
			helpers.ErrorLogger.Log(helpers.ERROR,
				"PublishWithBatchSyncAck failed to publish, %v\n", pubErr)
			return pubErr
		}
		// Wait for batch confirms
		for i = 0; i < ep.PubConfirmBatchSize; i++ {
			confirm, ok := <-confirms
			if !ok {
				err := fmt.Errorf("confirmation channel closed unexpectedly during batch %v", batchIdx)
				helpers.ErrorLogger.Log(helpers.ERROR,
					"PublishWithBatchSyncAck failed to publish, %v\n", err)
				return err
			}
			if confirm.Ack {
				helpers.DebugLogger.Log(helpers.DEBUG,
					"Batch sync ack delivery tag - %d", confirm.DeliveryTag)
			} else {
				helpers.DebugLogger.Log(helpers.DEBUG,
					"Batch sync NOT ack delivery tag - %d", confirm.DeliveryTag)
			}
		}

	}

	return pubErr
}

func (ep *ExpProducer) DeterminePublishQueue(testParams params.TestParams, allQueues AllQueues) error {

	bc := NewBasicConsumer(ep.Client)
	qAssignQueue := CreateQueueName(constants.PROD_Q_ASSIGN_Q, constants.PROD_Q_ASSIGN_Q_INDEX)
	pubQIndex, err := bc.FindQueueIndex(qAssignQueue, allQueues)
	if err != nil {
		return err
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"DeterminePublishQueue: Q assignment received = %v", *pubQIndex)
	var pubQName string
	var replyQName string
	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_PATTERN {
			pubQName = CreateQueueName(constants.THROUGHPUT_Q, *pubQIndex)
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN {
			pubQName = "" //empty queue name for fanout, producer publishes to an exchange
		}
	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
			pubQName = CreateQueueName(constants.LATENCY_REQUEST_Q, *pubQIndex)
			replyQName = CreateQueueName(constants.LATENCY_REPLY_Q, testParams.Experiment.RunnerID)
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			pubQName = ""
			replyQName = CreateQueueName(constants.LATENCY_REPLY_Q, constants.DEFAULT_Q_INDEX)

		}
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"DeterminePublishQueue: determined publish queue = %v", pubQName)
	ep.PubQueue = allQueues.Queues[pubQName]
	ep.ReplyQueue = allQueues.Queues[replyQName]
	return nil
}

func (ep *ExpProducer) DetermineExpectedReplyCount(testParams params.TestParams) {
	if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
		ep.ExpectedReplyCount = testParams.Experiment.MsgCount
	} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
		ep.ExpectedReplyCount = testParams.Experiment.MsgCount * testParams.Workload.NumConsumers
	}
}

func (ep *ExpProducer) ProducerConsumeReply(testParams params.TestParams, allQueues AllQueues) error {

	replyRecieveClient, err := NewClient(ep.Connector)
	if err != nil {
		return err
	}
	defer replyRecieveClient.Ch.Close()

	replyConsumer := NewBasicConsumer(replyRecieveClient)
	replyConsumer.ConsQueue = allQueues.Queues[ep.ReplyQueue.QName]

	replyConsumer.ConsPrefetchCount = constants.REPLY_Q_PROD_PREFETCH_COUNT
	replyConsumer.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	replyConsumer.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH

	replyConsumer.BasicConsumeArgs = &BasicConsumeArgs{}
	replyConsumer.ConsAutoAck = true
	replyConsumer.ConsExclusive = true
	replyConsumer.ConsNoLocal = false
	replyConsumer.ConsNoWait = false
	replyConsumer.ConsArgs = nil

	ep.DetermineExpectedReplyCount(testParams)

	err = replyConsumer.SetChannelQoS()
	if err != nil {
		return err
	}
	//start consumption
	err = replyConsumer.BasicConsume()
	if err != nil {
		return err
	}

	for replyConsumer.ConsRawMessage = range replyConsumer.ConsRcvdMsgs {
		ep.ReplyMsgCount += 1
		helpers.DebugLogger.Log(helpers.DEBUG, "Received reply %d of size: %v on %s queue\n",
			ep.ReplyMsgCount, len(replyConsumer.ConsRawMessage.Body), replyConsumer.ConsQueue.Q.Name)

		sendTime := ep.CorrIDStore.Retrieve(replyConsumer.ConsRawMessage.CorrelationId)
		if sendTime.IsZero() {
			err = fmt.Errorf("correlationID not found")
			helpers.ErrorLogger.Log(helpers.ERROR,
				"Failed to receive a reply: %v\n", err)
			return err
		}

		ep.Rtt.RttMap[replyConsumer.ConsRawMessage.CorrelationId] = append(ep.Rtt.RttMap[replyConsumer.ConsRawMessage.CorrelationId], time.Since(sendTime))
		//fmt.Printf("corrid: %v, sendTime: %v, rtt: %v\n", d.CorrelationId, sendTime, results.RttMap[d.CorrelationId])

		if testParams.Workload.DataPackaging.Type == constants.INDIVIDUAL_SEND {
			if ep.ReplyMsgCount >= ep.ExpectedReplyCount {
				//fmt.Println("break from receive loop")
				break
			}
		} else if testParams.Workload.DataPackaging.Type == constants.BATCH_SEND {
			if ep.ReplyMsgCount >= ep.ExpectedReplyCount/testParams.Workload.DataPackaging.BatchMessageCount {
				break
			}
		}

	}

	return nil
}

func (ep *ExpProducer) ProducerSendRtt(allQueues AllQueues) error {
	var err error
	var rttPubClient *Client
	//var totalMsgCount int64

	rttPubClient, err = NewClient(ep.Connector)
	if err != nil {
		return err
	}
	defer rttPubClient.Ch.Close()

	bp := NewBasicProducer(rttPubClient)

	bp.PubExchange = constants.EMPTY_EX_NAME
	bp.PubMandatory = false
	bp.PubImmediate = false
	bp.PubMsgDurability = amqp.Transient
	bp.PubMsgContentType = constants.PLAINTEXT_CONTENT
	pubQueue := CreateQueueName(constants.RESULT_Q, constants.RESULT_Q_INDEX)
	bp.PubQueue = allQueues.Queues[pubQueue]

	//bp.MonitorChannelClosure()

	//helpers.InfoLogger.Log(helpers.INFO, "Avg Rtt %v\n", ep.Rtt.AvgRtt)
	//helpers.InfoLogger.Log(helpers.INFO, "Rtt Vals %v\n", ep.Rtt.RttVals)

	//sending avg rtt
	//msgToBeSend := []byte(fmt.Sprintf("%d", ep.Rtt.AvgRtt))
	msgToBeSend := []byte(ep.Rtt.AvgRtt.String())
	//helpers.DebugLogger.Log(helpers.DEBUG, "Avg Rtt: %v\n", msgToBeSend)
	bp.PubMessage = amqp.Publishing{
		DeliveryMode: bp.PubMsgDurability,
		ContentType:  bp.PubMsgContentType,
		Body:         msgToBeSend}

	err = bp.BasicPublish()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"ProducerSendRtt failed to publish avg rtt, %v\n", err)
		return err
	}

	//sending all rtt vals
	//msgToBeSend = []byte(helpers.TimeDurationToString(ep.Rtt.RttVals))
	//helpers.DebugLogger.Log(helpers.DEBUG, "All Rtts: %v\n", msgToBeSend)
	msgToBeSend = helpers.TimeDurationsToByte(ep.Rtt.RttVals)
	bp.PubMessage = amqp.Publishing{
		DeliveryMode: bp.PubMsgDurability,
		ContentType:  bp.PubMsgContentType,
		Body:         msgToBeSend}

	err = bp.BasicPublish()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR,
			"ProducerSendRtt failed to publish all rtt vals, %v\n", err)
		return err
	}

	return nil

}

func (ep *ExpProducer) DeterminePubExchange(testParams params.TestParams) {
	if testParams.Experiment.TestPattern == constants.WORK_SHARING_PATTERN ||
		testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
		ep.BasicProducer.PubExchange = constants.EMPTY_EX_NAME
	} else if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN ||
		testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
		ep.BasicProducer.PubExchange = CreateExName(constants.BROADCAST_EX, constants.BROADCAST_EX_INDEX)
	}
}

func (ep *ExpProducer) ProducerRunStart(testParams params.TestParams, allQueues AllQueues) error {
	var err error

	//generate workload
	msg, err := testParams.Workload.GenerateWorkload()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "failed to generate a workload: %v\n", err)
		return err
	}

	//packaging
	MsgToBeSend := ep.DetermineMsgPackaging(testParams, msg)

	//set msg durability aka deliverymode
	ep.DetermineMsgDurability(testParams)

	//set contenttype
	ep.DetermineMsgContentType(testParams)

	//determine pub msg count, according to data packaging
	ep.DeterminePubMsgCount(testParams)

	//quorum parameters
	ep.PubQueueStructure = testParams.Tunables.GetQueueStructure()
	//ep.PubQueueQuorum = IsQuorumQueueRequired(testParams)
	ep.PubQueueOverflowMode = testParams.Tunables.GetQueueOverflow()

	//set publish properties
	ep.DeterminePubExchange(testParams)
	ep.BasicProducer.PubMandatory = false //TODO:true to capture returns, not working
	ep.BasicProducer.PubImmediate = false

	ep.BasicProducer.PubMessage = amqp.Publishing{
		DeliveryMode: ep.BasicProducer.PubMsgDurability,
		ContentType:  ep.BasicProducer.PubMsgContentType,
		Body:         []byte(MsgToBeSend)}

	//flow control
	/*
		flowCh := make(chan bool, 1)
		ep.BasicProducer.Ch.NotifyFlow(flowCh)
		ep.PublishAllowed.Store(true)

		go func() {
			for f := range flowCh {
				if f {
					helpers.InfoLogger.Log(helpers.INFO, "Flow=ON (server asked to pause)")
					ep.PublishAllowed.Store(false)
				} else {
					helpers.WarnLogger.Log(helpers.WARN, "Flow=OFF (server asked to resume)")
					ep.PublishAllowed.Store(true)
				}
			}
		}()
	*/

	//start consumption of replies if this is a latency test

	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		ep.ProdThreadCount = 0
	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		ep.ProdThreadCount = 1
	}

	errCh := make(chan error, ep.ProdThreadCount)
	doneCh := make(chan struct{}, ep.ProdThreadCount)

	if testParams.Experiment.TestType == constants.LATENCY_TEST {
		go func() {
			err = ep.ProducerConsumeReply(testParams, allQueues)
			if err != nil {
				errCh <- err
			}
			doneCh <- struct{}{}
		}()
	}

	//publish based on ack settings
	ep.PubConfirmMode, ep.PubConfirmBatchSize = testParams.Tunables.GetPublisherConfirms()
	switch ep.PubConfirmMode {
	case constants.PUB_NO_ACKS:
		err = ep.PublishWithNoAck(testParams)
		if err != nil {
			return err
		}
	case constants.PUB_INDIVIDUAL_ASYNC:
		err = ep.PublishWithIndividualAsyncAck(testParams)
		if err != nil {
			return err
		}
	case constants.PUB_INDIVIDUAL_SYNC:
		err = ep.PublishWithIndividualSyncAck(testParams)
		if err != nil {
			return err
		}
	case constants.PUB_BATCH_SYNC:
		err = ep.PublishWithBatchSyncAck(testParams)
		if err != nil {
			return err
		}
	default:
		helpers.ErrorLogger.Log(helpers.ERROR, "Unknown publisher ack mode: %v\n", err)
		return err
	}

	//wait for reply reception
	completed := 0
	for completed < ep.ProdThreadCount {
		select {
		case err := <-errCh:
			if err != nil {
				return err // return on first error
			}
		case <-doneCh:
			completed++
		}
	}

	//calculate avg Rtt and send avg and all Rtts to coordinator
	if testParams.Experiment.TestType == constants.LATENCY_TEST {
		ep.Rtt.CalculateAverageRtt()
		err = ep.ProducerSendRtt(allQueues)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ep *ExpProducer) RunProducer(testParams params.TestParams, allQueues AllQueues) error {

	var err error

	helpers.DebugLogger.Log(helpers.DEBUG,
		"RunProducer: Starting producer ...\n")

	//determine PubQueue
	err = ep.DeterminePublishQueue(testParams, allQueues)
	if err != nil {
		return err
	}

	ep.MonitorChannelClosure()
	//TODO: added to check if queue overflowed messages can be captured in returns
	//not working
	//ep.NotifyPublishReturns()

	err = ep.ProducerRunStart(testParams, allQueues)
	if err != nil {
		return err
	}

	return nil
}
