package main

import (
	"dstream-sim/arguments"
	"dstream-sim/constants"
	"dstream-sim/experiment"
	"dstream-sim/framework/rabbitmq"
	"dstream-sim/helpers"
	"dstream-sim/params"
	"dstream-sim/run"
	setup "dstream-sim/setup/provision"
	"dstream-sim/workload"

	"cogentcore.org/lab/base/mpi"
)

func main() {

	var err error
	//parse args
	simIns := &arguments.SimulatorIns{}
	err = simIns.ParseArgs()
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Test params parsing error: %v\n", err)
		return
	}
	helpers.DebugLogger.Log(helpers.DEBUG, "SimIns: %v\n", simIns)

	testParams := &params.TestParams{}
	testParams.Framework = simIns.Framework
	testParams.Cleanup = simIns.Cleanup
	testParams.ExtraPort = simIns.Extraport

	if testParams.Framework == constants.RABBITMQ {
		testParams.Connection = &rabbitmq.ConnectionConfig{}
	}

	//setup
	if *simIns.ClusterConfigFile != "" {
		clusterIn := &setup.ClusterIn{}
		err = clusterIn.ParseClusterConfigIn(simIns.ClusterConfigFile)
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cluster config parsing error: %v\n", err)
			return
		}

		err = clusterIn.ProvisionCluster()
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cluster provisioning error: %v\n", err)
			return
		}
		//TODO:Need to populate the connConfig struct from the provisiocluster output

	} else {
		// if clusterconfig is not specified:
		// parse framework configs file
		err = testParams.Connection.ParseConnectionConfig(simIns)
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Connection config parsing error: %v\n", err)
			return
		}
	}

	//parse workload config
	testParams.Workload = &workload.WorkloadConfig{}
	err = testParams.Workload.ParseWorkloadConfig(simIns)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Workload config parsing error: %v\n", err)
		return
	}

	//Initialize MPI comm
	if testParams.Workload.ProducerParallelism == constants.MPI_PARALLELISM {
		mpi.Init()
		defer mpi.Finalize()

	}

	//parse experiment config
	testParams.Experiment = &experiment.ExperimentConfig{}
	err = testParams.Experiment.ParseExperimentConfig(simIns)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Experiment config parsing error: %v\n", err)
		return
	}

	//parse framework tunables config
	testParams.Tunables = &rabbitmq.Tunables{}
	err = testParams.Tunables.ParseTunablesConfig(simIns)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Tunables config parsing error: %v\n", err)
		return
	}

	//start experiment
	err = run.RunParamsExperiment(testParams)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to run the experiment: %v\n", err)
		return
	}

}
