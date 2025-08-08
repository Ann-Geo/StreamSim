package rabbitmq

import (
	"dstream-sim/constants"
	"dstream-sim/helpers"
	"dstream-sim/params"

	"cogentcore.org/lab/base/mpi"
)

type MpiConsumer struct {
	*ExpConsumer
	Comm      *mpi.Comm
	WorldSize int
	WorldRank int
}

func NewMpiConsumer(testParams params.TestParams, client *Client) (*MpiConsumer, error) {

	comm, err := mpi.NewComm(nil)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "MPI communicator initialization failed, %v\n", err)
		return nil, err
	}

	worldSize := mpi.WorldSize()
	//fmt.Println("# MPI procs: ", worldSize)

	worldRank := mpi.WorldRank()
	//fmt.Println("Rank: ", worldRank)
	return &MpiConsumer{
		ExpConsumer: NewExpConsumer(testParams, client),
		Comm:        comm,
		WorldSize:   worldSize,
		WorldRank:   worldRank,
	}, nil
}

func (mp *MpiConsumer) DetermineConsumeQueue(testParams params.TestParams, allQueues AllQueues) error {
	var err error
	qAssignQueue := CreateQueueName(constants.CONS_Q_ASSIGN_Q, constants.CONS_Q_ASSIGN_Q_INDEX)
	mp.ConsQueueIndex, err = mp.FindQueueIndex(qAssignQueue, allQueues)
	if err != nil {
		return err
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"DetermineConsumeQueue: Q assignment received = %v", *mp.ConsQueueIndex)

	var consQName string
	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_PATTERN {
			consQName = CreateQueueName(constants.THROUGHPUT_Q, *mp.ConsQueueIndex)
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN {
			consQName = CreateQueueName(constants.THROUGHPUT_Q, testParams.Experiment.RunnerID)
		}
	} else if testParams.Experiment.TestType == constants.LATENCY_TEST {
		if testParams.Experiment.TestPattern == constants.WORK_SHARING_FB_PATTERN {
			consQName = CreateQueueName(constants.LATENCY_REQUEST_Q, *mp.ConsQueueIndex)
		} else if testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			consQName = CreateQueueName(constants.LATENCY_REQUEST_Q, testParams.Experiment.RunnerID)
		}
	}

	helpers.DebugLogger.Log(helpers.DEBUG,
		"DetermineConsumeQueue: determined consume queue = %v", consQName)

	mp.ConsQueue = allQueues.Queues[consQName]
	return nil
}

func (mc *MpiConsumer) RunConsumer(testParams params.TestParams, allQueues AllQueues) error {
	var err error

	helpers.DebugLogger.Log(helpers.DEBUG,
		"RunConsumer: Starting consumer ...\n")
	//determine queue
	err = mc.DetermineConsumeQueue(testParams, allQueues)
	if err != nil {
		return err
	}

	barrierErr := mc.Comm.Barrier()
	if barrierErr != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "RunConsumer: MPI Barrier error\n")
	}
	helpers.DebugLogger.Log(helpers.DEBUG,
		"RunConsumer: Barrier done\n")

	mc.ConsQueueStructure = testParams.Tunables.GetQueueStructure()
	//ec.ConsQueueQuorum = IsQuorumQueueRequired(testParams)
	mc.DetermineConsMsgCountExpected(testParams)

	mc.MonitorChannelClosure()

	err = mc.ConsumerRunStart(testParams, allQueues)
	if err != nil {
		return err
	}

	return nil

}
