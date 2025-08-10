package rabbitmq

import (
	"StreamSim/constants"
	"StreamSim/helpers"
	"StreamSim/params"
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Coordinator struct {
	*BasicConsumer
	*BasicProducer
	QueueAssignments []int64
	CoordThreadCount int
	SumRtt           time.Duration
	AvgRtt           time.Duration
	RttVals          []time.Duration
}

func NewCoordinator(client *Client) *Coordinator {
	return &Coordinator{
		BasicConsumer: NewBasicConsumer(client),
		BasicProducer: NewBasicProducer(client),
	}
}

// only function that uses Coordinator's own channel
func (co *Coordinator) MessageCountMonitor(testParams params.TestParams, allQueues AllQueues, allExchanges AllExchanges) error {

	var err error

	//consuming the message count
	consQueue := CreateQueueName(constants.CONS_MSGCOUNT_Q, constants.CONS_MSGCOUNT_Q_INDEX)
	co.ConsQueue = allQueues.Queues[consQueue]
	co.ConsPrefetchCount = constants.MSGCOUNT_Q_CONS_PREFETCH_COUNT
	co.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	co.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH

	co.BasicConsumeArgs = &BasicConsumeArgs{}
	co.ConsAutoAck = true
	co.ConsExclusive = true
	co.ConsNoLocal = false
	co.ConsNoWait = false
	co.ConsArgs = nil
	err = co.SetChannelQoS()
	if err != nil {
		return err
	}
	//start consumption
	err = co.BasicConsume()
	if err != nil {
		return err
	}

	//create a publisher for publishing shutdown message to shutdown listener
	//bp := NewBasicProducer(co.Client)
	/*
		shutdownExchange := CreateExName(constants.CONS_SHUTDOWN_EX,
			constants.CONS_SHUTDOWN_EX_INDEX)
		co.PubExchange = allExchanges.Exchanges[shutdownExchange].ExName
		co.PubMandatory = false
		co.PubImmediate = false
		co.PubMsgDurability = amqp.Transient
		co.PubMsgContentType = constants.PLAINTEXT_CONTENT
		//TODO:Need to set this properly later
		co.PubQueue.QName = constants.EMPTY_Q_NAME
		co.PubMessage = amqp.Publishing{
			DeliveryMode: co.PubMsgDurability,
			ContentType:  co.PubMsgContentType,
			Body:         []byte(constants.SHUTDOWN_SIGNAL)}
	*/

	//receive msgcount messages
	var count int64
	//var firstTS, lastTS int64
	//MsgCount setting should be equal in coordinator and consumer config files
	var totalMsgCount int64
	if testParams.Workload.DataPackaging.Type == constants.BATCH_SEND {
		totalMsgCount = (testParams.Experiment.MsgCount * testParams.Workload.NumConsumers) /
			testParams.Workload.DataPackaging.BatchMessageCount
	} else if testParams.Workload.DataPackaging.Type == constants.INDIVIDUAL_SEND {
		totalMsgCount = testParams.Experiment.MsgCount * testParams.Workload.NumConsumers
	}

	for co.ConsRawMessage = range co.ConsRcvdMsgs {
		//parts := strings.Split(string(co.ConsRawMessage.Body), ",")
		//if len(parts) != 2 {
		//err = fmt.Errorf("invalid msg, should be the format of <1,timestamp>")
		//helpers.ErrorLogger.Log(helpers.ERROR,
		//"RunCoordinator received invalid message, %v\n", err)
		//return err
		//}
		//ts, err := strconv.ParseInt(parts[1], 10, 64)
		//if err != nil {
		//helpers.ErrorLogger.Log(helpers.ERROR,
		//"RunCoordinator could not parse timestamp, %v\n", err)
		//return err
		//}
		//if count == 0 {
		//firstTS = ts
		//}
		//lastTS = ts

		singleConsCount, err := strconv.ParseInt(string(co.ConsRawMessage.Body), 10, 64)
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR,
				"RunCoordinator could not parse timestamp, %v\n", err)
			return err
		}
		count = count + singleConsCount

		helpers.InfoLogger.Log(helpers.INFO,
			"RunCoordinator: MsgCount till now: %v\n", count)

		if count >= totalMsgCount {
			//duration := float64(lastTS-firstTS) / 1e9
			//throughput := float64(count) / duration
			helpers.InfoLogger.Log(helpers.INFO,
				"RunCoordinator: MsgCount global limit reached, Total messages: %v\n", count)

			/*
				err = co.BasicPublish()
				if err != nil {
					helpers.ErrorLogger.Log(helpers.ERROR, "RunCoordinator failed to publish shutdown message: %v\n", err)
					return err
				}
			*/
			break
		}
	}
	return nil
}

func (co *Coordinator) HandleRttMessages(body []byte) error {
	payload := strings.TrimSpace(string(body))

	if strings.Contains(payload, ",") {
		// Multiple values
		strs := strings.Split(payload, ",")
		//durations := make([]time.Duration, 0, len(strs))
		for _, s := range strs {
			d, err := time.ParseDuration(strings.TrimSpace(s))
			if err != nil {
				err = fmt.Errorf("failed to parse rtt vals: %s", s)
				helpers.ErrorLogger.Log(helpers.ERROR, "HandleRttMessages: Rtt parsing failed: %v\n", err)
				return err
			}
			co.RttVals = append(co.RttVals, d)
		}
	} else {
		// Single value
		d, err := time.ParseDuration(payload)
		if err != nil {
			err = fmt.Errorf("invalid RTT format: %s", payload)
			helpers.ErrorLogger.Log(helpers.ERROR, "HandleRttMessages: Rtt parsing failed: %v\n", err)
			return err
		}
		co.SumRtt += d
	}
	return nil
}

func (co *Coordinator) RunResultCalculator(testParams params.TestParams, allQueues AllQueues) error {
	var rcvdMsgCount int64
	var err error
	var aggTh float64
	var consQueue string

	resultCalculatorClient, err := NewClient(co.BasicConsumer.Connector)
	if err != nil {
		return err
	}
	defer resultCalculatorClient.Ch.Close()

	bc := NewBasicConsumer(resultCalculatorClient)
	bc.ConsPrefetchSize = constants.CONS_PREFETCH_SIZE
	bc.ConsGlobalPrefetch = constants.CONS_GLOBAL_PREFETCH

	bc.BasicConsumeArgs = &BasicConsumeArgs{}
	bc.ConsAutoAck = true
	bc.ConsExclusive = true
	bc.ConsNoLocal = false
	bc.ConsNoWait = false
	bc.ConsArgs = nil

	consQueue = CreateQueueName(constants.RESULT_Q, constants.RESULT_Q_INDEX)
	bc.ConsQueue = allQueues.Queues[consQueue]
	bc.ConsPrefetchCount = constants.RESULT_Q_PREFETCH_COUNT

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
		rcvdMsgCount = rcvdMsgCount + 1

		if len(bc.ConsRawMessage.Body) < 8 {
			err = fmt.Errorf("invalid message size for result")
			helpers.ErrorLogger.Log(helpers.ERROR, "RunResultCalculator: Throughput calculation failed %v\n", err)
			return err
		}

		if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {

			floatStr := string(bc.ConsRawMessage.Body)
			val, err := strconv.ParseFloat(floatStr, 64) // convert string to float64
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR, "RunResultCalculator: Throughput calculation failed %v\n", err)
				return err
			}

			aggTh += val
			helpers.DebugLogger.Log(helpers.DEBUG,
				"RunResultCalculator: Each consumer throughput received, %v\n", val)

			if rcvdMsgCount == testParams.Workload.NumConsumers {
				helpers.InfoLogger.Log(helpers.INFO,
					"RunResultCalculator: Aggregate Throughput: %v\n", aggTh)
				break
			}
		} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
			err = co.HandleRttMessages(bc.ConsRawMessage.Body)
			if err != nil {
				return err
			}

			//each producer will send avg rtt and all rtt vals as well
			if rcvdMsgCount == testParams.Workload.NumProducers*2 {
				helpers.InfoLogger.Log(helpers.INFO,
					"RunResultCalculator: Average Rtt: %v\n", co.SumRtt/time.Duration(testParams.Workload.NumProducers))
				//helpers.InfoLogger.Log(helpers.INFO,
				//"RunResultCalculator: All Rtt vals: %v\n", co.RttVals)
				//write rttvals to a file - append only
				rttValsFilePath := constants.RESULT_STORE_PATH + testParams.Connection.GetToolkit() + "_" +
					testParams.Workload.WorkloadName + "_" +
					strconv.FormatInt(testParams.Workload.NumConsumers, 10) + ".txt"
				err = helpers.AppendDurationsToFile(rttValsFilePath, co.RttVals)
				if err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func (co *Coordinator) GenerateQueueAssignments(testParams params.TestParams) {
	var i int64
	totalQueues := testParams.Tunables.GetTotalQueues()
	//TODO: Assuming numproducers and numconsumers are equal
	//single producer, multi consumer test for broadcast pattern need to be handled
	numConsumers := testParams.Workload.NumConsumers

	assignments := make([]int64, numConsumers)
	for i = 0; i < numConsumers; i++ {
		assignments[i] = i * totalQueues / numConsumers
	}
	co.QueueAssignments = assignments
	helpers.DebugLogger.Log(helpers.DEBUG,
		"GenerateQueueAssignments: queue assignments = %v\n", assignments)
}

func (co *Coordinator) QueueIndexpublish(qName string, allQueues AllQueues) error {

	queueIdxPubClient, err := NewClient(co.BasicProducer.Connector)
	if err != nil {
		return err
	}
	defer queueIdxPubClient.Ch.Close()

	bp := NewBasicProducer(queueIdxPubClient)
	bp.PubQueue = allQueues.Queues[qName]
	bp.PubExchange = constants.EMPTY_EX_NAME
	//CorrIDStore *MessageStore
	bp.PubMandatory = false
	bp.PubImmediate = false
	bp.PubMsgDurability = amqp.Transient
	bp.PubMsgContentType = constants.PLAINTEXT_CONTENT
	for idx, assign := range co.QueueAssignments {
		assignIndex, err := helpers.Int64ToBytesLE(assign)
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cannot resolve endiannness: %v\n", err)
		}
		helpers.DebugLogger.Log(helpers.DEBUG,
			"QueueIndexpublish: assignIndex %v is %v\n", idx, assignIndex)
		bp.PubMessage = amqp.Publishing{
			DeliveryMode: co.PubMsgDurability,
			ContentType:  co.PubMsgContentType,
			Body:         []byte(assignIndex)}

		err = bp.BasicPublish()
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR,
				"QueueCoordinator failed to publish queue assignment, %v\n", err)
			return err
		}
	}
	return nil
}

func (co *Coordinator) QueueCoordinator(testParams params.TestParams, allQueues AllQueues) error {
	var err error
	co.GenerateQueueAssignments(testParams)

	pubQueue := CreateQueueName(constants.PROD_Q_ASSIGN_Q, constants.PROD_Q_ASSIGN_Q_INDEX)
	err = co.QueueIndexpublish(pubQueue, allQueues)
	if err != nil {
		return err
	}

	consQueue := CreateQueueName(constants.CONS_Q_ASSIGN_Q, constants.CONS_Q_ASSIGN_Q_INDEX)
	err = co.QueueIndexpublish(consQueue, allQueues)
	if err != nil {
		return err
	}

	return nil
}

func (co *Coordinator) RunCoordinator(testParams params.TestParams, allQueues AllQueues, allExchanges AllExchanges) error {
	var err error

	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		co.CoordThreadCount = 2
	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		co.CoordThreadCount = 2
	}

	errCh := make(chan error, co.CoordThreadCount)
	doneCh := make(chan struct{}, co.CoordThreadCount)

	go func() {
		err = co.MessageCountMonitor(testParams, allQueues, allExchanges)
		if err != nil {
			errCh <- err
		}
		doneCh <- struct{}{}
	}()

	go func() {
		err = co.RunResultCalculator(testParams, allQueues)
		if err != nil {
			errCh <- err
		}
		doneCh <- struct{}{}
	}()

	err = co.QueueCoordinator(testParams, allQueues)
	if err != nil {
		return err
	}

	completed := 0
	for completed < co.CoordThreadCount {
		select {
		case err := <-errCh:
			if err != nil {
				return err // return on first error
			}
		case <-doneCh:
			completed++
		}
	}

	helpers.DebugLogger.Log(helpers.DEBUG, "Coordinator exiting!!!")
	return nil
}
