package rabbitmq

import (
	"StreamSim/helpers"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QExBinder struct {
	QExBindingKey   string
	QExBinderNoWait bool
	QExBinderArgs   amqp.Table
}

func (b *QExBinder) BasicBind(q Queue, e Exchange, client Client) error {

	err := client.Ch.QueueBind(
		q.QName,
		b.QExBindingKey, // routing key
		e.ExName,        // exchange
		b.QExBinderNoWait,
		b.QExBinderArgs,
	)

	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Failed to bind queue to an exchange: %v\n", err)
		return err
	}
	return nil

}
