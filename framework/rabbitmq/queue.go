package rabbitmq

import (
	"StreamSim/constants"
	"StreamSim/helpers"
	"StreamSim/params"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
// queue defining charas
	workload - done
	ConsumptionMode *string `json:"ConsumptionMode"` // push, pull
	only push for now. RMQ basically follows a push based messaging model.
	a polling, pull based model is highly inefficient:
	https://www.rabbitmq.com/docs/consumers#polling

	experiment - done
	//queue declare
	TestType - DetermineQueueType


	tunables
	//sending
		PubConfirms
			batch size
		Message durability


	//reception
		Consumer Ack

	//queue declare - done
	Queue Durability        Durability       `json:"Durability"`
	QueueProperties   *QueueProperties `json:"QueueProperties,omitempty"`
	TotalQueues       int              `json:"TotalQueues"`

	//consumer initialization
	PrefetchCount     *PrefetchCount   `json:"PrefetchCount,omitempty"`

	//not now
	BindingDurability string           `json:"BindingDurability"`


	//TODO: Add QueueType later - not now
	StreamUseCase     *StreamUseCase   `json:"StreamUseCase,omitempty"`
*/

type Queue struct {
	Q                 amqp.Queue
	QName             string
	QType             string
	QDurability       bool
	QDeleteWhenUnused bool
	QExclusive        bool
	QNoWait           bool
	QArgs             amqp.Table
	QDeletionArgs     QDeletionArgs
	QStructure        string
	//QQuorum           bool

}

type QDeletionArgs struct {
	IfUnused bool
	IfEmpty  bool
	IfNoWait bool
}
type AllQueues struct {
	Queues map[string]Queue
}

func NewAllQueues(testParams params.TestParams) *AllQueues {
	return &AllQueues{
		Queues: make(map[string]Queue),
	}
}

/*
func IsQuorumQueueRequired(testParams params.TestParams) bool {
	quorum := (testParams.Workload.PayloadSize * testParams.Experiment.MsgCount *
		testParams.Workload.NumProducers) > constants.MAX_MEMORY_QUEUE_SIZE
	if quorum {
		helpers.DebugLogger.Log(helpers.DEBUG, "Quorum queues required\n")
	}
	return quorum
}
*/

func (q *Queue) DetermineQueueDurability(testParams params.TestParams) {
	queueDurability := testParams.Tunables.GetQueueDurability()
	if queueDurability == constants.PERSISTENT {
		q.QDurability = true
	} else if queueDurability == constants.NONE { //in-memory queue
		q.QDurability = false
	}

	if q.QType == constants.LATENCY_REPLY_Q {
		q.QDurability = false
	} else if q.QType == constants.CONS_DEADLETTER_Q {
		q.QDurability = false
	} else if q.QType == constants.RESULT_Q {
		q.QDurability = false
	} else if q.QType == constants.CONS_MSGCOUNT_Q {
		q.QDurability = false
	} else if q.QType == constants.CONS_SHUTDOWN_Q {
		q.QDurability = false
	} else if q.QType == constants.PROD_Q_ASSIGN_Q {
		q.QDurability = false
	} else if q.QType == constants.CONS_Q_ASSIGN_Q {
		q.QDurability = false
	}
	//fmt.Println("c.QueueName", c.QueueName)
}

func (q *Queue) GetQueueSizeMessages(testParams params.TestParams, ae AllExchanges, index int64) error {
	var err error
	if q.QType == constants.THROUGHPUT_Q ||
		q.QType == constants.LATENCY_REQUEST_Q ||
		q.QType == constants.LATENCY_REPLY_Q {

		q.QStructure = testParams.Tunables.GetQueueStructure()
		qOverflowType := testParams.Tunables.GetQueueOverflow()
		queueSize := (constants.MAX_MEMORY_QUEUE_SIZE / testParams.Workload.PayloadSize) /
			testParams.Workload.NumProducers
		helpers.DebugLogger.Log(helpers.DEBUG, "Source queue size: %v\n", queueSize)
		if q.QStructure == constants.CLASSIC_Q {
			if qOverflowType == constants.Q_OVERFLOW_TYPE_DROP_HEAD {
				q.QArgs = amqp.Table{
					"x-max-length": int32(queueSize),
					//TODO: x-overflow default is drop-head, which is at most once
					//if set to dead-letter, will give at least once
					//other configs required: dead-letter-strategy and dead-letter-exchange
					//Ref: https://www.rabbitmq.com/blog/2022/03/29/at-least-once-dead-lettering
				}
			} else if qOverflowType == constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH {
				q.QArgs = amqp.Table{
					"x-queue-type": constants.CLASSIC_Q,
					"x-max-length": testParams.Tunables.GetQueueSizeMessages(), //int32(queueSize), //TODO: change this to queueSize
					"x-overflow":   constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH,
				}
			} else if qOverflowType == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
				err = fmt.Errorf("unsupported overflow type by rabbitmq")
				helpers.ErrorLogger.Log(helpers.ERROR, "Unsupported overflow type for classic queue: %v\n", qOverflowType)
				return err
			}
		} else if q.QStructure == constants.QUORUM_Q { //in case of quorum queue limit the queue length to

			if qOverflowType == constants.Q_OVERFLOW_TYPE_DEAD_LETTER {
				helpers.DebugLogger.Log(helpers.DEBUG, "Setting args for quorum dead letter queue\n")
				dlxExName := ae.Exchanges[CreateExName(constants.CONS_DEADLETTER_EX, index)].ExName
				dlqQueueName := CreateQueueName(constants.CONS_DEADLETTER_Q, index)
				q.QArgs = amqp.Table{
					"x-max-length":              int32(queueSize),
					"x-queue-type":              constants.QUORUM_Q,
					"x-dead-letter-exchange":    dlxExName,
					"x-overflow":                constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH,
					"x-dead-letter-routing-key": dlqQueueName,
					"dead-letter-strategy":      constants.DEAD_LETTER_STRATEGY,
				}
			} else if qOverflowType == constants.Q_OVERFLOW_TYPE_DROP_HEAD {
				q.QArgs = amqp.Table{
					"x-max-length": int32(queueSize),
					"x-queue-type": constants.QUORUM_Q,
				}
			} else if qOverflowType == constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH {

				q.QArgs = amqp.Table{
					"x-max-length": int32(queueSize), //TODO: change this to queueSize
					"x-queue-type": constants.QUORUM_Q,
					"x-overflow":   constants.Q_OVERFLOW_TYPE_REJECT_PUBLISH,
				}
			}
		} else if q.QStructure == constants.STREAM_Q {
			q.QArgs = amqp.Table{
				"x-max-length-bytes": constants.MAX_MEMORY_QUEUE_SIZE,
				"x-queue-type":       constants.STREAM_Q,
			}
		}

		//setting msgcount queue size to per producer msg count * num producers
	} else if q.QType == constants.CONS_DEADLETTER_Q {
		queueSize := (constants.MAX_MEMORY_QUEUE_SIZE / testParams.Workload.PayloadSize) /
			testParams.Workload.NumProducers
		q.QArgs = amqp.Table{
			"x-max-length": int32(queueSize),
		}
	} else if q.QType == constants.CONS_MSGCOUNT_Q {
		q.QArgs = amqp.Table{
			"x-max-length": testParams.Workload.NumConsumers,
		}
	} else if q.QType == constants.CONS_SHUTDOWN_Q {
		q.QArgs = amqp.Table{
			"x-max-length": constants.CONS_SHUTDOWN_Q_MAX_LENGTH,
		}
	} else if q.QType == constants.PROD_Q_ASSIGN_Q {
		helpers.DebugLogger.Log(helpers.DEBUG,
			"GetQueueSizeMessages: prod q assign q queue size: %v\n", testParams.Workload.NumProducers)
		q.QArgs = amqp.Table{
			"x-max-length": testParams.Workload.NumProducers,
		}
	} else if q.QType == constants.CONS_Q_ASSIGN_Q {
		helpers.DebugLogger.Log(helpers.DEBUG,
			"GetQueueSizeMessages: cons q assign q queue size: %v\n", testParams.Workload.NumConsumers)
		q.QArgs = amqp.Table{
			"x-max-length": testParams.Workload.NumConsumers,
		}
	} else if q.QType == constants.RESULT_Q {
		helpers.DebugLogger.Log(helpers.DEBUG,
			"GetQueueSizeMessages: result q queue size: %v\n", testParams.Workload.NumConsumers*2)
		q.QArgs = amqp.Table{
			"x-max-length": testParams.Workload.NumConsumers * 2,
		}
	}
	return nil
}

// Init all queues
func (aq *AllQueues) InitAllQueues(testParams params.TestParams, client Client, ae AllExchanges) error {
	var err error
	var i int64 = 0

	//for throughput aggregatations from consumers
	q := &Queue{QType: constants.RESULT_Q}
	err = q.QueueTypeInitialize(testParams, ae, client, constants.RESULT_Q_INDEX)
	if err != nil {
		return err
	}
	aq.Queues[q.QName] = *q

	//initialize prod queue assignment queue
	q = &Queue{QType: constants.PROD_Q_ASSIGN_Q}
	err = q.QueueTypeInitialize(testParams, ae, client, constants.PROD_Q_ASSIGN_Q_INDEX)
	if err != nil {
		return err
	}
	aq.Queues[q.QName] = *q

	//initialize cons queue assignment queue
	q = &Queue{QType: constants.CONS_Q_ASSIGN_Q}
	err = q.QueueTypeInitialize(testParams, ae, client, constants.CONS_Q_ASSIGN_Q_INDEX)
	if err != nil {
		return err
	}
	aq.Queues[q.QName] = *q

	/*
		//initialize consumer shutdown queue
		q = &Queue{QType: constants.CONS_SHUTDOWN_Q}
		err = q.QueueTypeInitialize(testParams, ae, client, testParams.Experiment.RunnerID)
		if err != nil {
			return err
		}
		aq.Queues[q.QName] = *q
	*/

	/*
		//only shutdown queue has exchange right now
		exName := CreateExName(constants.CONS_SHUTDOWN_EX, constants.CONS_SHUTDOWN_EX_INDEX)
		e := ae.Exchanges[exName]
		qExbinder := &QExBinder{QExBinderNoWait: false, QExBinderArgs: nil}
		err = qExbinder.BasicBind(*q, e, client)
		if err != nil {
			return err
		}
	*/

	//initialize consumers msg count aggregation queue for
	//consumer shutdown condition
	q = &Queue{QType: constants.CONS_MSGCOUNT_Q}
	err = q.QueueTypeInitialize(testParams, ae, client, constants.CONS_MSGCOUNT_Q_INDEX)
	if err != nil {
		return err
	}
	aq.Queues[q.QName] = *q

	/*
		//dead letter queues
		for i = range testParams.Tunables.GetTotalQueues() {
			q := &Queue{QType: constants.CONS_DEADLETTER_Q}
			err = q.QueueTypeInitialize(testParams, ae, client, i)
			if err != nil {
				return err
			}
			aq.Queues[q.QName] = *q

			//bind dead letter queue with dead letter exchange
			exName = CreateExName(constants.CONS_DEADLETTER_EX, i)
			e = ae.Exchanges[exName]
			qExbinder = &QExBinder{QExBinderNoWait: false, QExBinderArgs: nil}
			err = qExbinder.BasicBind(*q, e, client)
			if err != nil {
				return err
			}
		}
	*/

	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {

		if testParams.Experiment.TestPattern == constants.WORK_SHARING_PATTERN {

			for i = range testParams.Tunables.GetTotalQueues() {
				q := &Queue{QType: constants.THROUGHPUT_Q}
				err = q.QueueTypeInitialize(testParams, ae, client, i)
				if err != nil {
					return err
				}
				aq.Queues[q.QName] = *q
			}
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN {
			if testParams.Experiment.Role == constants.CONSUMER_ROLE {
				q := &Queue{QType: constants.THROUGHPUT_Q}
				err = q.QueueTypeInitialize(testParams, ae, client, testParams.Experiment.RunnerID)
				if err != nil {
					return err
				}
				aq.Queues[q.QName] = *q
				exName := CreateExName(constants.BROADCAST_EX, constants.BROADCAST_EX_INDEX)
				e := ae.Exchanges[exName]
				qExbinder := &QExBinder{QExBindingKey: q.QName, QExBinderNoWait: false, QExBinderArgs: nil}
				err = qExbinder.BasicBind(*q, e, client)
				if err != nil {
					return err
				}
			}
		}

	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
			for i := range testParams.Tunables.GetTotalQueues() {
				q := &Queue{QType: constants.LATENCY_REQUEST_Q}
				err = q.QueueTypeInitialize(testParams, ae, client, i)
				if err != nil {
					return err
				}
				aq.Queues[q.QName] = *q

			}

			if testParams.Experiment.Role == constants.PRODUCER_ROLE {
				q = &Queue{QType: constants.LATENCY_REPLY_Q}
				err = q.QueueTypeInitialize(testParams, ae, client, testParams.Experiment.RunnerID)
				if err != nil {
					return err
				}
				aq.Queues[q.QName] = *q

				exName := CreateExName(constants.PROD_REPLY_EX, constants.PROD_REPLY_EX_INDEX)
				e := ae.Exchanges[exName]
				qExbinder := &QExBinder{QExBindingKey: q.QName, QExBinderNoWait: false, QExBinderArgs: nil}
				err = qExbinder.BasicBind(*q, e, client)
				if err != nil {
					return err
				}
			}
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			if testParams.Experiment.Role == constants.CONSUMER_ROLE {
				q := &Queue{QType: constants.LATENCY_REQUEST_Q}
				err = q.QueueTypeInitialize(testParams, ae, client, testParams.Experiment.RunnerID)
				if err != nil {
					return err
				}
				aq.Queues[q.QName] = *q
				exName := CreateExName(constants.BROADCAST_EX, constants.BROADCAST_EX_INDEX)
				e := ae.Exchanges[exName]
				qExbinder := &QExBinder{QExBindingKey: q.QName, QExBinderNoWait: false, QExBinderArgs: nil}
				err = qExbinder.BasicBind(*q, e, client)
				if err != nil {
					return err
				}
			}

			q = &Queue{QType: constants.LATENCY_REPLY_Q}
			err = q.QueueTypeInitialize(testParams, ae, client, constants.DEFAULT_Q_INDEX)
			if err != nil {
				return err
			}
			aq.Queues[q.QName] = *q
		}
	}

	return nil
}

// should be called for each type of queue
func (q *Queue) QueueTypeInitialize(testParams params.TestParams, ae AllExchanges, client Client, index int64) error {

	var err error
	q.QExclusive = false
	q.QNoWait = false

	q.DetermineQueueDurability(testParams)
	//q.QQuorum = IsQuorumQueueRequired(testParams)
	err = q.GetQueueSizeMessages(testParams, ae, index)
	if err != nil {
		return err
	}

	if q.QType == constants.CONS_DEADLETTER_Q {
		q.QDeleteWhenUnused = false
	} else if q.QType == constants.RESULT_Q {
		q.QDeleteWhenUnused = false
	} else if q.QType == constants.PROD_Q_ASSIGN_Q {
		q.QDeleteWhenUnused = false

	} else if q.QType == constants.CONS_Q_ASSIGN_Q {
		q.QDeleteWhenUnused = false

	} else if q.QType == constants.CONS_SHUTDOWN_Q {
		q.QDeleteWhenUnused = true
		q.QExclusive = true

	} else if q.QType == constants.CONS_MSGCOUNT_Q {
		q.QDeleteWhenUnused = false

	} else if q.QType == constants.THROUGHPUT_Q {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_PATTERN {
			q.QDeleteWhenUnused = false
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN {
			q.QDeleteWhenUnused = true
			q.QExclusive = true
		}

	} else if q.QType == constants.LATENCY_REQUEST_Q {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
			q.QDeleteWhenUnused = false
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			q.QDeleteWhenUnused = true
			q.QExclusive = true
		}

	} else if q.QType == constants.LATENCY_REPLY_Q {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
			q.QDeleteWhenUnused = true
			q.QExclusive = true
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			q.QDeleteWhenUnused = false //can be true also since only one producer is consuming from it
		}
	}
	//creating queuename
	q.QName = CreateQueueName(q.QType, index)

	err = q.BasicQueueDeclare(testParams, client)
	if err != nil {
		return err
	}

	//making queues delete/purge ready
	q.MakeQueuesDeleteReady()

	return nil
}

func CreateQueueName(qType string, index int64) string {
	return fmt.Sprintf("%s_%d", qType, index)
}

func (q *Queue) BasicQueueDeclare(testParams params.TestParams, client Client) error {
	var err error
	q.Q, err = client.Ch.QueueDeclare(
		q.QName,             // name
		q.QDurability,       // durable
		q.QDeleteWhenUnused, // delete when unused
		q.QExclusive,        // exclusive
		q.QNoWait,           // no-wait
		q.QArgs,             // arguments
	)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to declare a queue: %v\n", err)
		return err
	}
	return nil
}

func (q *Queue) MakeQueuesDeleteReady() {
	q.QDeletionArgs.IfUnused = false
	q.QDeletionArgs.IfEmpty = false
	q.QDeletionArgs.IfNoWait = false
}

func (aq *AllQueues) DeleteAllQueues(testParams params.TestParams, client Client) error {

	for key := range aq.Queues {
		helpers.DebugLogger.Log(helpers.DEBUG, "Found queue: %v\n", key)
		//TODO: Check this condition for multi queue tests
		//Need to revisit this if broadcast also start using multi queues
		if testParams.Experiment.TestType == constants.THROUGHPUT_TEST &&
			testParams.Tunables.GetTotalQueues() > 1 &&
			!strings.Contains(aq.Queues[key].QName, constants.THROUGHPUT_Q) {
			helpers.DebugLogger.Log(helpers.DEBUG, "Deleting queue: %v\n", key)
			_, err := client.Ch.QueueDelete(aq.Queues[key].QName,
				aq.Queues[key].QDeletionArgs.IfUnused,
				aq.Queues[key].QDeletionArgs.IfEmpty,
				aq.Queues[key].QDeletionArgs.IfNoWait)
			if err != nil {
				helpers.ErrorLogger.Log(helpers.ERROR, "Failed to purge a queue: %v\n", err)
				return err
			}
			//}
		}
	}
	return nil
}
