package params

import (
	"StreamSim/experiment"
	"StreamSim/framework"
	"StreamSim/workload"
)

type TestParams struct {
	Framework  string //which framework to use
	Experiment *experiment.ExperimentConfig
	Connection framework.ConnectionConfig
	Workload   *workload.WorkloadConfig
	Tunables   framework.Tunables
	Cleanup    bool
	ExtraPort  string
}
