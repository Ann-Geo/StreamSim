package rabbitmq

import (
	"StreamSim/constants"
	"StreamSim/helpers"
	"StreamSim/params"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	ExName       string
	ExCustomType string
	ExType       string
	ExDurability bool //true, the exchange survives broker restarts
	ExAutoDelete bool //true, the exchange is deleted when the last queue unbinds from it
	ExInternal   bool //true this exchange can’t be published to by clients,
	// used internally by rmq
	ExNoWait bool //true, the server does not send a confirmation response,
	// which means we don't know if the declare succeeded or failed
	ExArgs amqp.Table
}

type AllExchanges struct {
	Exchanges map[string]Exchange
}

func NewAllExchanges(testParams params.TestParams) *AllExchanges {
	return &AllExchanges{
		Exchanges: make(map[string]Exchange),
	}
}

func (e *Exchange) BasicExchangeDeclare(testParams params.TestParams, client Client) error {

	err := client.Ch.ExchangeDeclare(
		e.ExName,
		e.ExType,
		e.ExDurability,
		e.ExAutoDelete,
		e.ExInternal,
		e.ExNoWait,
		e.ExArgs,
	)

	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to declare a exchange: %v\n", err)
		return err
	}
	return nil
}

func (ae *AllExchanges) InitAllExchanges(testParams params.TestParams, client Client) error {
	//var i int64
	/*
		//initialize consumer shutdown exchage
		e := &Exchange{ExCustomType: constants.CONS_SHUTDOWN_EX}
		err := e.ExchangeTypeInitialize(testParams, client, constants.CONS_SHUTDOWN_EX_INDEX)
		if err != nil {
			return err
		}
		ae.Exchanges[e.ExName] = *e

		//initialize deadletter exchange for quorum queues
		for i = range testParams.Tunables.GetTotalQueues() {
			e = &Exchange{ExCustomType: constants.CONS_DEADLETTER_EX}
			err = e.ExchangeTypeInitialize(testParams, client, i)
			if err != nil {
				return err
			}
			ae.Exchanges[e.ExName] = *e
		}
	*/

	e := &Exchange{ExCustomType: constants.PROD_REPLY_EX}
	err := e.ExchangeTypeInitialize(testParams, client, constants.PROD_REPLY_EX_INDEX)
	if err != nil {
		return err
	}
	ae.Exchanges[e.ExName] = *e

	if testParams.Experiment.TestType == constants.THROUGHPUT_TEST ||
		testParams.Experiment.TestType == constants.LATENCY_TEST {

		if testParams.Experiment.TestPattern == constants.BROADCAST_PATTERN ||
			testParams.Experiment.TestPattern == constants.BROADCAST_GATHER_PATTERN {
			e = &Exchange{ExCustomType: constants.BROADCAST_EX}
			err = e.ExchangeTypeInitialize(testParams, client, constants.BROADCAST_EX_INDEX)
			if err != nil {
				return err
			}
			ae.Exchanges[e.ExName] = *e
		}

	}

	return nil
}

func (e *Exchange) ExchangeTypeInitialize(testParams params.TestParams, client Client, index int64) error {

	var err error
	e.ExDurability = true
	e.ExAutoDelete = false
	e.ExInternal = false
	e.ExNoWait = false
	e.ExArgs = nil

	if e.ExCustomType == constants.CONS_SHUTDOWN_EX {
		e.ExType = constants.FANOUT_EX
		e.ExName = CreateExName(constants.CONS_SHUTDOWN_EX, index)
		helpers.DebugLogger.Log(helpers.DEBUG, "Exchange name is: %v\n", e.ExName)

	} else if e.ExCustomType == constants.CONS_DEADLETTER_EX {
		e.ExType = constants.FANOUT_EX
		e.ExName = CreateExName(constants.CONS_DEADLETTER_EX, index)
		helpers.DebugLogger.Log(helpers.DEBUG, "Exchange name is: %v\n", e.ExName)

	} else if e.ExCustomType == constants.PROD_REPLY_EX {
		e.ExType = constants.DIRECT_EX
		e.ExName = CreateExName(constants.PROD_REPLY_EX, index)
		helpers.DebugLogger.Log(helpers.DEBUG, "Exchange name is: %v\n", e.ExName)
	} else if e.ExCustomType == constants.BROADCAST_EX {
		e.ExType = constants.FANOUT_EX
		e.ExName = CreateExName(constants.BROADCAST_EX, index)
		helpers.DebugLogger.Log(helpers.DEBUG, "Exchange name is: %v\n", e.ExName)
	}

	err = e.BasicExchangeDeclare(testParams, client)
	if err != nil {
		return err
	}
	return nil
}

func CreateExName(exCustomType string, index int64) string {
	return fmt.Sprintf("%s_%d", exCustomType, index)
}
