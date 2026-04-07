package tunnel

import (
	"github.com/vitalvas/mqttv5"
	"github.com/vitalvas/mqttv5/extensions/router"
)

const qos = mqttv5.QoS1

type MessageHandler func(topic string, payload []byte)

type Transport interface {
	Publish(topic string, payload []byte) error
	Subscribe(filter string, handler MessageHandler) error
	SubscribeAll() error
	Unsubscribe(filters ...string) error
	IsConnected() bool
	Close() error
	ClientID() string
}

type MQTTTransport struct {
	client *mqttv5.Client
	router *router.Router
}

func NewMQTTTransport(client *mqttv5.Client) *MQTTTransport {
	return &MQTTTransport{
		client: client,
		router: router.New(),
	}
}

func (t *MQTTTransport) Publish(topic string, payload []byte) error {
	return t.client.Publish(&mqttv5.Message{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
	})
}

func (t *MQTTTransport) Subscribe(filter string, handler MessageHandler) error {
	t.router.Handle(func(msg *mqttv5.Message) {
		handler(msg.Topic, msg.Payload)
	}, router.WithTopic(filter), router.WithSubscribeQoS(byte(qos)))

	return nil
}

func (t *MQTTTransport) SubscribeAll() error {
	return t.router.Subscribe(t.client, byte(qos))
}

func (t *MQTTTransport) Unsubscribe(filters ...string) error {
	t.router.Clear()

	return t.client.Unsubscribe(filters...)
}

func (t *MQTTTransport) IsConnected() bool {
	return t.client.IsConnected()
}

func (t *MQTTTransport) Close() error {
	return t.client.Close()
}

func (t *MQTTTransport) ClientID() string {
	return t.client.ClientID()
}
