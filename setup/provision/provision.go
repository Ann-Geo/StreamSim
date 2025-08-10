package setup

import (
	"StreamSim/helpers"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func (p *ClusterIn) ProvisionCluster() error {

	var cmdExec *exec.Cmd
	var deployCmd = PROVISION_SCRIPT + " " + DEPLOY_FLAG + " " + p.ClusterType + " " +
		p.ClusterName + " " + strconv.FormatUint(p.NumNodes, 10)
	var infoCmd = PROVISION_SCRIPT + " " + INFO_FLAG + " " + p.ClusterType + " " +
		p.ClusterName + " " + strconv.FormatUint(p.NumNodes, 10)
	var shutdownCmd = PROVISION_SCRIPT + " " + SHUTDOWN_FLAG + " " + p.ClusterType + " " +
		p.ClusterName + " " + strconv.FormatUint(p.NumNodes, 10)

	var cdio ClusterDIOut
	var cso ClusterShutdownOut
	var output []byte
	var err error

	//cd ../olcf-s3m-python-main
	err = os.Chdir("../olcf-s3m-python-main/")
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to change directory")
		return err
	}

	if p.DeployCluster {

		helpers.DebugLogger.Log(helpers.DEBUG, "Provisioning the cluster: %v\n", deployCmd)
		args := strings.Fields(deployCmd)
		cmdExec = exec.Command("python", args...)

		output, err = cmdExec.CombinedOutput()
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cannot execute cluster deploy command: %v\n %s\n", err, output)
			return err
		}

		//don't parse the deploy Cmd output because output format is different from info command
		/*
			_, err = ParseClusterOut(&cdio, output)
			if err != nil {
				return err
			}
		*/

		//successful output
		helpers.DebugLogger.Log(helpers.DEBUG, "Cluster deploy output: %v", output)

		//Always check info after deploy to see if cluster state is healthy
		p.InfoCluster = true
	}

	for ((cdio.HealthStatus != CLUSTER_HEALTHY) && (cdio.HealthStatus != "")) || p.InfoCluster {
		helpers.DebugLogger.Log(helpers.DEBUG, "Checking info of cluster: %v\n", infoCmd)

		fmt.Println(p.InfoCluster)
		fmt.Println(cdio.HealthStatus)

		args := strings.Fields(infoCmd)
		cmdExec = exec.Command("python", args...)

		output, err := cmdExec.CombinedOutput()
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cannot execute cluster info command: %v\n %s\n", err, output)
			return err
		}

		errFoundFlag, err := ParseClusterOut(&cdio, output)
		if err != nil {
			return err
		}

		fmt.Println("errFoundFlag", errFoundFlag)

		//bad cluster, prevent further checks
		if errFoundFlag {
			p.InfoCluster = false
		} else {
			if cdio.HealthStatus == CLUSTER_HEALTHY {
				p.InfoCluster = false
			}
		}

		time.Sleep(2 * time.Second)
	}

	if p.ShutdownCluster {
		helpers.DebugLogger.Log(helpers.DEBUG, "Shutting down the cluster: %v\n", shutdownCmd)
		args := strings.Fields(shutdownCmd)
		fmt.Println(args)
		cmdExec = exec.Command("python", args...)

		output, err := cmdExec.CombinedOutput()
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cannot execute cluster shutdown command: %v\n %s\n", err, output)
			return err
		}

		_, err = ParseClusterOut(&cso, output)
		if err != nil {
			return err
		}
	}

	//cd ../StreamSim
	err = os.Chdir("../StreamSim/")
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to change directory")
		return err
	}

	return nil
}
