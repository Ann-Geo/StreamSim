package run

import (
	"dstream-sim/constants"
	"dstream-sim/framework/rabbitmq"
	"dstream-sim/helpers"
	"dstream-sim/params"
)

func RunParamsExperiment(t *params.TestParams) error {

	var err error
	var conn *rabbitmq.Connector
	var cli *rabbitmq.Client

	//create connector
	conn, err = rabbitmq.NewConnector(*t)
	if err != nil {
		return err
	}
	defer conn.Conn.Close()

	//create client
	cli, err = rabbitmq.NewClient(conn)
	if err != nil {
		return err
	}
	defer cli.Ch.Close()

	//initialize all exchanges
	ae := rabbitmq.NewAllExchanges(*t)
	err = ae.InitAllExchanges(*t, *cli)
	if err != nil {
		return err
	}

	//initialize all queues
	aq := rabbitmq.NewAllQueues(*t)
	err = aq.InitAllQueues(*t, *cli, *ae)
	if err != nil {
		return err
	}
	if t.Experiment.Role == constants.COORDINATOR_ROLE {
		if t.Cleanup {
			helpers.DebugLogger.Log(helpers.DEBUG, "Cleanup flag set: %v\n", t.Cleanup)
			err = aq.DeleteAllQueues(*cli)
			if err != nil {
				return err
			}
			return nil
		}
		coord := rabbitmq.NewCoordinator(cli)
		err = coord.RunCoordinator(*t, *aq, *ae)
		if err != nil {
			return err
		}

		//cleanup
		//delete all queues - only coordinator should do this
		/*
			err = aq.DeleteAllQueues(*cli)
			if err != nil {
				return err
			}
		*/

	} else if t.Experiment.Role == constants.PRODUCER_ROLE {

		if t.Workload.ProducerParallelism == constants.MPI_PARALLELISM {
			var mpiProd *rabbitmq.MpiProducer
			mpiProd, err = rabbitmq.NewMpiProducer(cli)
			if err != nil {
				return err
			}
			err = mpiProd.RunProducer(*t, *aq)
		} else {
			expProd := rabbitmq.NewExpProducer(cli)
			err = expProd.RunProducer(*t, *aq)
		}

		if err != nil {
			return err
		}

	} else if t.Experiment.Role == constants.CONSUMER_ROLE {

		if t.Workload.ConsumerParallelism == constants.MPI_PARALLELISM {
			var mpiCons *rabbitmq.MpiConsumer
			mpiCons, err = rabbitmq.NewMpiConsumer(*t, cli)
			if err != nil {
				return err
			}
			err = mpiCons.RunConsumer(*t, *aq)
		} else {
			cons := rabbitmq.NewExpConsumer(*t, cli)
			err = cons.RunConsumer(*t, *aq)
		}
		if err != nil {
			return err
		}
	}

	return nil
}
