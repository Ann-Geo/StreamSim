package setup

import (
	"bytes"
	"dstream-sim/helpers"
	"encoding/json"
	"errors"
	"os"
)

type ClusterIn struct {
	ClusterType     string
	ClusterName     string
	NumNodes        uint64
	DeployCluster   bool
	ShutdownCluster bool
	InfoCluster     bool
}

func (c *ClusterIn) ParseClusterConfigIn(clusterConfigFile *string) error {

	data, err := os.ReadFile(*clusterConfigFile)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Error reading cluster config file: %v\n", err)
		return err
	}

	if err := json.Unmarshal(data, &c); err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Error unmarshaling cluster config file: %v\n", err)
		return err
	}

	// Validate only one flag is specified.
	flagCount := 0
	if c.DeployCluster {
		flagCount++
	}
	if c.ShutdownCluster {
		flagCount++
	}
	if c.InfoCluster {
		flagCount++
	}
	if flagCount != 1 {
		helpers.ErrorLogger.Log(helpers.ERROR, "More than one flags specified among deploy, info, shutdown: %v\n", err)
		return err
	}

	helpers.DebugLogger.Log(helpers.DEBUG, "Parsed cluster configuration:\n%+v\n", c)

	return nil
}

const (
	PROVISION_SCRIPT = "olcf-s3m-streaming.py"
	DEPLOY_FLAG      = "-d"
	INFO_FLAG        = "-i"
	SHUTDOWN_FLAG    = "-s"
	CLUSTER_HEALTHY  = "healthy"

	CLUSTER_NOT_FOUND = "Not Found (404)"
)

type Lifetime struct {
	ProvisionedAt    string  `json:"provisionedAt"`
	ExtendedAt       *string `json:"extendedAt"`
	SecondsLifetime  int     `json:"secondsLifetime"`
	SecondsRemaining int     `json:"secondsRemaining"`
}

type ResourceSettings struct {
	Cpus   int `json:"cpus"`
	Nodes  int `json:"nodes"`
	RamGbs int `json:"ram-gbs"`
}

type ClusterDIOut struct { //for deploy and info
	Kind             string           `json:"kind"`
	Name             string           `json:"name"`
	SyncStatus       string           `json:"syncStatus"`
	HealthStatus     string           `json:"healthStatus"`
	Lifetime         Lifetime         `json:"lifetime"`
	ResourceSettings ResourceSettings `json:"resourceSettings"`
	Username         string           `json:"username"`
	Password         string           `json:"password"`
	AmqpsUrl         string           `json:"amqpsUrl"`
	MgmtUrl          string           `json:"mgmtUrl"`
}

type ClusterShutdownOut struct {
	Success bool `json:"success"`
}

// generics, requires go: 1.18+
func ParseClusterOut[T any](clusterOutTarget *T, output []byte) (bool, error) {
	var errFoundFlag bool = true
	//Not Found (404)
	errFoundPattern := []byte(CLUSTER_NOT_FOUND)
	if !bytes.Contains(output, errFoundPattern) {
		errFoundFlag = false
		jsonStart := bytes.IndexByte(output, '{')
		if jsonStart == -1 {
			return errFoundFlag, errors.New("no JSON object found in the output")
		}
		jsonData := output[jsonStart:]
		err := json.Unmarshal(jsonData, clusterOutTarget)
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Cluster output parse error: %v\n", err)

			return errFoundFlag, err
		}
		helpers.DebugLogger.Log(helpers.DEBUG, "Cluster output: %v\n", string(jsonData))
	} else {
		helpers.DebugLogger.Log(helpers.DEBUG, "Cluster not found.\n Cluster output: %v\n", string(output))
		errFoundFlag = true
	}
	return errFoundFlag, nil

}
