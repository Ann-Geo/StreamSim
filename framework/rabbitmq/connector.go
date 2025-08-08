package rabbitmq

import (
	"crypto/tls"
	"dstream-sim/constants"
	"dstream-sim/helpers"
	"dstream-sim/params"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connector struct {
	Conn *amqp.Connection
}

func NewConnector(testParams params.TestParams) (*Connector, error) {
	var err error
	connector := &Connector{}

	err = connector.Connect(testParams)
	if err != nil {
		return nil, err
	}

	return connector, nil
}

func (conn *Connector) Connect(testParams params.TestParams) error {
	var err error

	amqpUrl := testParams.Connection.GetAmqpsUrl()

	//check for additional ports to use in case of scistream - only for producer
	if testParams.Experiment.Role == constants.PRODUCER_ROLE {
		if testParams.Connection.GetToolkit() == constants.SCISTREAM {
			portToUse := testParams.ExtraPort
			if portToUse != "" {
				amqpUrl = helpers.UpdatePortInUrl(amqpUrl, portToUse)
			}
		}
	}

	if testParams.Connection.GetTlsEncryption() == constants.TLS_ENCRYPT {

		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}

		c := amqp.Config{
			TLSClientConfig: tlsConfig,
		}

		conn.Conn, err = amqp.DialConfig(amqpUrl, c)
	} else if testParams.Connection.GetTlsEncryption() == constants.NO_TLS_ENCRYPT {
		conn.Conn, err = amqp.Dial(amqpUrl)
	}
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to connect to RabbitMQ: %v\n", err)
		return err
	}

	helpers.DebugLogger.Log(helpers.DEBUG, "Connected to RabbitMQ using %v !\n", amqpUrl)
	//fmt.Println("Connected to RMQ")
	return nil
}
