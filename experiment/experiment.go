package experiment

import (
	"StreamSim/arguments"
	"StreamSim/helpers"
	"encoding/json"
	"os"
)

type ExperimentConfig struct {
	Role        string //producer or consumer
	RunnerID    int64
	Duration    int64   // in minutes
	MsgCount    int64   //per producer or per consumer
	NumPhases   []int64 // each phase gets a Workload or tunables
	Concurrency int64   // Producer/consumer concurrency separation in minutes
	TestType    string  //latency test or throughput test or non-test
	//non-test option was later added to declare queues for result management,
	//consumer termination etc.
	TestPattern string //for testing different messaging patterns
	TestMode    string //count based or duration based
}

func (e *ExperimentConfig) ParseExperimentConfig(simIns *arguments.SimulatorIns) error {
	bytes, err := os.ReadFile(*simIns.ExperimentConfigFile)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Error reading experiment config file: %v\n", err)
		return err
	}

	err = json.Unmarshal(bytes, &e)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to unmarshal experiment config file: %v\n", err)
		return err
	}

	/*
		if e.Role == "" {
			err := fmt.Errorf("must have a role defined")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get role: %v\n", err)
			return err
		}
		if e.Duration == 0 {
			err := fmt.Errorf("experiment must have a duration")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get duration: %v\n", err)
			return err
		}
		if len(e.NumPhases) == 0 {
			err := fmt.Errorf("experiment must have at least 1 phase")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get experiment phases: %v\n", err)
			return err
		}
		if e.Concurrency > e.Duration {
			err := fmt.Errorf("experiment concurrency must be less than its duration")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get experiment concurrency: %v\n", err)
			return err
		}
	*/

	//unique id for producer or consumer instance running the experiment
	e.RunnerID = helpers.RandomID()
	helpers.DebugLogger.Log(helpers.DEBUG, "RunnerId: %v\n", e.RunnerID)

	return nil
}
