package arguments

import (
	"StreamSim/helpers"
	"flag"
	"fmt"
	"os"
)

type SimulatorIns struct {
	Framework            string  //which framework to use
	ExperimentConfigFile *string // Experiment run specifcs
	FrameworkConfigFile  *string // Framework connection config
	WorkloadConfigFile   *string // Workload characteristics
	ClusterConfigFile    *string // Cluster provision inputs
	TunablesConfigFile   *string // Framework specific tunables
	Cleanup              bool    //clean all queues
	Extraport            string  //additional ports for scistream toolkit
}

func (si *SimulatorIns) ParseArgs() error {

	framework := flag.String("f", "", "Framework to use")
	si.ExperimentConfigFile = flag.String("e", "", "Path to experiment config file")
	si.FrameworkConfigFile = flag.String("fc", "", "Path to framework config file")
	si.WorkloadConfigFile = flag.String("w", "", "Path to workload config file")
	si.ClusterConfigFile = flag.String("c", "", "Path to cluster config file (optional)")
	si.TunablesConfigFile = flag.String("t", "", "Path to tunables config file")
	cleanup := flag.Bool("cleanup", false, "If true, clean all queues before start")
	extraPort := flag.String("port", "", "Additional port for scistream")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	si.Framework = *framework
	si.Cleanup = *cleanup
	si.Extraport = *extraPort

	helpers.DebugLogger.Log(helpers.DEBUG, "Cleanup input: %v\n", si.Cleanup)

	return si.ValidateInput()
}

func (si *SimulatorIns) ValidateInput() error {
	// Optional check: if cluster config file is provided, verify it exists
	if si.ClusterConfigFile != nil && *si.ClusterConfigFile != "" {
		if !helpers.FileExists(*si.ClusterConfigFile) {
			err := fmt.Errorf("cluster config file does not exist: %s", *si.ClusterConfigFile)
			helpers.ErrorLogger.Log(helpers.ERROR, "Validating simulator Ins failed: %v\n", err)
			return err
		}
	}

	// Validate other optional config files
	for _, file := range []*string{
		si.ExperimentConfigFile,
		si.FrameworkConfigFile,
		si.WorkloadConfigFile,
		si.TunablesConfigFile,
	} {
		if file != nil && *file != "" && !helpers.FileExists(*file) {
			err := fmt.Errorf("file does not exist: %s", *file)
			helpers.ErrorLogger.Log(helpers.ERROR, "Validating simulator Ins failed: %v\n", err)
			return err
		}
	}

	return nil
}
