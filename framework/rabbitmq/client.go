package rabbitmq

import (
	"dstream-sim/helpers"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	*Connector
	Ch        *amqp.Channel
	CPChClose chan bool //TODO: Need multiple, don't know yet
}

func NewClient(connector *Connector) (*Client, error) {
	var err error
	client := &Client{}

	client.Connector = connector
	err = client.OpenChannel()
	if err != nil {
		return nil, err
	}

	client.CPChClose = make(chan bool)
	return client, nil

}

func (cli *Client) OpenChannel() error {
	//opens a channel on the established connection, API interactions
	//happen through this channel

	ch, err := cli.Conn.Channel()
	if err != nil {
		return err
	}

	cli.Ch = ch

	//fmt.Println("Opened a channel in RMQ")
	return nil
}

func (cli *Client) ChCloseNotify() {
	go func() {
		<-cli.Ch.NotifyClose(make(chan *amqp.Error))
		cli.CPChClose <- true
	}()
}

func (cli *Client) MonitorChannelClosure() {
	errChan := cli.Ch.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err := <-errChan
		if err != nil {
			helpers.ErrorLogger.Log(helpers.ERROR, "Channel closed! Code=%d Reason=%s", err.Code, err.Reason)
		}
		if cli.CPChClose != nil {
			cli.CPChClose <- true
		}
	}()
}
