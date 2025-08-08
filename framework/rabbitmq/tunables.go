/*
type Rabbitmqtunables struct {
	//metadata handling?
	//other reliability features?
}
*/

package rabbitmq

import (
	"dstream-sim/arguments"
	"dstream-sim/constants"
	"dstream-sim/helpers"
	"encoding/json"
	"fmt"
	"os"
)

type Tunables struct {
	Acknowledgements  Acknowledgements `json:"Acknowledgements"`
	Durability        Durability       `json:"Durability"`
	PrefetchCount     *PrefetchCount   `json:"PrefetchCount,omitempty"`
	QueueProperties   *QueueProperties `json:"QueueProperties,omitempty"`
	BindingDurability string           `json:"BindingDurability"`
	TotalQueues       int64            `json:"TotalQueues"`
	//TODO: Add QueueType later
	StreamUseCase *StreamUseCase `json:"StreamUseCase,omitempty"`
}

type Acknowledgements struct {
	PublisherConfirms PublisherConfirms `json:"PublisherConfirms"`
	ConsumerAcks      ConsumerAcks      `json:"ConsumerAcks"`
}

type PublisherConfirms struct {
	Type      string `json:"type"`
	BatchSize *int64 `json:"batchSize,omitempty"`
}

type ConsumerAcks struct {
	Mode                   string  `json:"mode"`                             // "automatic" or "manual"
	ManualAckType          *string `json:"manualAckType,omitempty"`          // required only if Mode == "manual"
	ManualAckMultipleCount int64   `json:"manualAckMultipleCount,omitempty"` // used only if ManualAckType == "multiple"
}

type Durability struct {
	MessageDurability string `json:"MessageDurability"`
	QueueDurability   string `json:"QueueDurability"`
}

type PrefetchCount struct {
	PerConsumer *int `json:"PerConsumer,omitempty"`
}

type QueueProperties struct {
	QueueStructure    string `json:"QueueStructure"`
	QueueSizeMessages int    `json:"QueueSizeMessages"`
	QueueOverflow     string `json:"QueueOverflow"`
}

type StreamUseCase struct {
	MaxLength           int `json:"MaxLength"`
	MaxSegmentSizeBytes int `json:"MaxSegmentSizeBytes"`
}

// ParseConfig parses a JSON file into the Config struct
func (t *Tunables) ParseTunablesConfig(simIns *arguments.SimulatorIns) error {
	file, err := os.ReadFile(*simIns.TunablesConfigFile)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Error reading tunables config file: %v\n", err)
		return err
	}

	err = json.Unmarshal(file, &t)
	if err != nil {

	}

	// Additional runtime validation (batchSize should only be set if type == "batch")
	if t.Acknowledgements.PublisherConfirms.Type != constants.PUB_BATCH_SYNC &&
		t.Acknowledgements.PublisherConfirms.BatchSize != nil {
		err := fmt.Errorf("batchSize must not be set when type is not 'batch'")
		helpers.ErrorLogger.Log(helpers.ERROR, "Tunables Ack batch size error: %v\n", err)
		return err
	}
	if t.Acknowledgements.PublisherConfirms.Type == constants.PUB_BATCH_SYNC &&
		t.Acknowledgements.PublisherConfirms.BatchSize == nil {
		err := fmt.Errorf("batchSize must be set when type is 'batch'")
		helpers.ErrorLogger.Log(helpers.ERROR, "Tunables Ack batch size error: %v\n", err)
		return err
	}

	return nil
}

func (t *Tunables) GetTotalQueues() int64 {

	return t.TotalQueues
}

func (t *Tunables) GetQueueDurability() string {

	return t.Durability.QueueDurability
}

func (t *Tunables) GetMessageDurability() string {

	return t.Durability.MessageDurability
}

func (t *Tunables) GetQueueSizeMessages() int {

	return t.QueueProperties.QueueSizeMessages
}

func (t *Tunables) GetPublisherConfirms() (string, int64) {
	confirmType := t.Acknowledgements.PublisherConfirms.Type
	var batchSize int64

	if t.Acknowledgements.PublisherConfirms.BatchSize != nil {
		batchSize = *t.Acknowledgements.PublisherConfirms.BatchSize
	} else {
		batchSize = 0 // or some other default, like 1
	}

	return confirmType, batchSize
}

func (t *Tunables) GetConsumerAcks() (string, string, int64) {
	mode := t.Acknowledgements.ConsumerAcks.Mode
	var ackType string
	var ackMultipleCount int64

	if t.Acknowledgements.ConsumerAcks.ManualAckType != nil {
		ackType = *t.Acknowledgements.ConsumerAcks.ManualAckType
		ackMultipleCount = t.Acknowledgements.ConsumerAcks.ManualAckMultipleCount
		helpers.DebugLogger.Log(helpers.DEBUG, "Manual ack count: %v\n", ackMultipleCount)
	} else {
		ackType = ""
	}

	return mode, ackType, ackMultipleCount
}

func (t *Tunables) GetPrefetchCount() int {

	return *t.PrefetchCount.PerConsumer
}

func (t *Tunables) GetQueueOverflow() string {

	return t.QueueProperties.QueueOverflow
}

func (t *Tunables) GetQueueStructure() string {

	return t.QueueProperties.QueueStructure
}
