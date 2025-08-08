package rabbitmq

import (
	"dstream-sim/helpers"

	"cogentcore.org/lab/base/mpi"
)

//Mpi producer has no meaning for input queue index.
//should distribute queues among them equally based on numqueues

type MpiProducer struct {
	*ExpProducer
	Comm      *mpi.Comm
	WorldSize int
	WorldRank int
}

func NewMpiProducer(client *Client) (*MpiProducer, error) {

	comm, err := mpi.NewComm(nil)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "MPI communicator initialization failed, %v\n", err)
		return nil, err
	}

	worldSize := mpi.WorldSize()
	//fmt.Println("# MPI procs: ", worldSize)

	worldRank := mpi.WorldRank()
	//fmt.Println("Rank: ", worldRank)
	return &MpiProducer{
		ExpProducer: NewExpProducer(client),
		Comm:        comm,
		WorldSize:   worldSize,
		WorldRank:   worldRank,
	}, nil
}

/*
func (mp *MpiProducer) DeterminePublishQueue(testParams params.TestParams) string {
	var pubQName string

	queueIndex := (int64(mp.WorldRank) * testParams.Tunables.GetTotalQueues()) / int64(mp.WorldSize)
	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		pubQName = CreateQueueName(constants.THROUGHPUT_Q, queueIndex)
	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		pubQName = CreateQueueName(constants.LATENCY_REQUEST_Q, queueIndex)
	}
	return pubQName
}

func (mp *MpiProducer) RunProducer(testParams params.TestParams, allQueues AllQueues) error {

	var err error

	//determine PubQueue
	mp.BasicProducer.PubQueue = allQueues.Queues[mp.DeterminePublishQueue(testParams)]

	err = mp.ProducerRunStart(testParams)
	if err != nil {
		return err
	}

	return nil
}
*/
