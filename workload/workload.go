package workload

import (
	"dstream-sim/arguments"
	"dstream-sim/helpers"
	"encoding/json"
	"fmt"
	"os"
)

type WorkloadConfig struct {
	//workload name
	WorkloadName string `json:"WorkloadName"`
	// generating charas
	PayloadSize    int64   `json:"PayloadSize"`    // in bytes
	PayloadFormat  string  `json:"PayloadFormat"`  // plaintext, json, binary, hdf5
	PayloadElement *string `json:"PayloadElement"` // image, file, variable ..

	// sending charas
	DataRate            int64                `json:"DataRate"`      // in KB/s
	DataPackaging       *DataPackagingConfig `json:"DataPackaging"` // now an object
	Variability         *string              `json:"Variability"`   // burst, steady, fluctuating
	NumProducers        int64                `json:"NumProducers"`
	ProducerParallelism string               `json:"ProducerParallelism"` // nonMPI

	// receiving charas
	NumConsumers            int64   `json:"NumConsumers"`
	ConsumerParallelism     string  `json:"ConsumerParallelism"`     // dedicatedMPI, nonMPI
	ConsPayloadDistribution *string `json:"ConsPayloadDistribution"` // roundrobin, random

	// queue defining charas
	ConsumptionMode *string `json:"ConsumptionMode"` // push, pull
}

type DataPackagingConfig struct {
	Type              string `json:"type"`                        // "batched" or "individual"
	BatchMessageCount int64  `json:"batchMessageCount,omitempty"` // only required if type == "batched"
}

func (w *WorkloadConfig) ParseWorkloadConfig(simIns *arguments.SimulatorIns) error {
	bytes, err := os.ReadFile(*simIns.WorkloadConfigFile)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Error reading workload config file: %v\n", err)
		return err
	}
	err = json.Unmarshal(bytes, &w)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to unmarshal workload config file: %v\n", err)
		return err
	}

	// Validation: if DataPackaging is "batched", batchMessageCount must be set
	if w.DataPackaging != nil {
		if w.DataPackaging.Type == "batched" && w.DataPackaging.BatchMessageCount == 0 {
			err := fmt.Errorf("batchMessageCount must be set when DataPackaging type is 'batched'")
			helpers.ErrorLogger.Log(helpers.ERROR, "Workload data packaging batch size error: %v\n", err)
			return err
		}
		if w.DataPackaging.Type != "batched" && w.DataPackaging.BatchMessageCount != 0 {
			err := fmt.Errorf("batchMessageCount should not be set when DataPackaging type is not 'batched'")
			helpers.ErrorLogger.Log(helpers.ERROR, "Workload data packaging  batch size error: %v\n", err)
			return err
		}
	}

	/*
		if w.PayloadSize == 0 {
			err := fmt.Errorf("must have a payload size")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get payload size: %v\n", err)
			return err
		}
		if *w.PayloadFormat == "" {
			err := fmt.Errorf("experiment must have a payload format")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get payload format: %v\n", err)
			return err
		}
		if *w.PayloadElement == "" {
			err := fmt.Errorf("experiment must have a payload element")
			helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get payload element: %v\n", err)
			return err
		}
	*/

	return nil
}

func (w *WorkloadConfig) GenerateWorkload() ([]byte, error) {
	var err error
	if w.PayloadSize == 0 {
		err = fmt.Errorf("payload size must be greater than 0")
		helpers.ErrorLogger.Log(helpers.ERROR, "Error in workload payloadsize: %v\n", err)
		return nil, err
	}

	format := "plaintext"
	format = w.PayloadFormat

	element := "file"
	element = *w.PayloadElement

	switch format {
	case "plaintext":
		return helpers.GeneratePlaintext(w.PayloadSize, element), nil
	case "json":
		return helpers.GenerateJSON(w.PayloadSize, element)
	case "binary":
		return helpers.GenerateBinary(w.PayloadSize, element), nil
	case "hdf5":
		return helpers.GenerateHDF5(w.PayloadSize, element), nil
	default:
		err = fmt.Errorf("unsupported payload format: %s", format)
		helpers.ErrorLogger.Log(helpers.ERROR, "Error in workload payloadsize: %v\n", err)
		return nil, err
	}
}
