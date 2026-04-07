package tunnel

import (
	"log/slog"

	"github.com/vitalvas/mqttv5"
	"github.com/vitalvas/mqttv5/extensions/router"
)

const defaultQoS = mqttv5.QoS1

type MessageHandler func(topic string, payload []byte)

type PubMessage struct {
	Topic         string
	Payload       []byte
	QoS           byte
	ContentType   string
	ResponseTopic string
}

type Transport interface {
	Publish(msg PubMessage) error
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
	logger *slog.Logger
}

func NewMQTTTransport(client *mqttv5.Client, logger *slog.Logger) *MQTTTransport {
	return &MQTTTransport{
		client: client,
		router: router.New(),
		logger: logger,
	}
}

func (t *MQTTTransport) Publish(msg PubMessage) error {
	t.logger.Debug("publish", "topic", msg.Topic, "qos", msg.QoS, "size", len(msg.Payload))

	m := &mqttv5.Message{
		Topic:         msg.Topic,
		Payload:       msg.Payload,
		QoS:           msg.QoS,
		ContentType:   msg.ContentType,
		ResponseTopic: msg.ResponseTopic,
	}

	return t.client.Publish(m)
}

func (t *MQTTTransport) Subscribe(filter string, handler MessageHandler) error {
	t.router.Handle(func(msg *mqttv5.Message) {
		handler(msg.Topic, msg.Payload)
	}, router.WithTopic(filter), router.WithSubscribeQoS(byte(defaultQoS)))

	return nil
}

func (t *MQTTTransport) SubscribeAll() error {
	filters := t.router.Filters()
	for _, f := range filters {
		t.logger.Debug("subscribing", "filter", f)
	}

	handler := t.router.MessageHandler()

	wrappedHandler := func(msg *mqttv5.Message) {
		t.logger.Debug("message received", "topic", msg.Topic, "size", len(msg.Payload))
		handler(msg)
	}

	for _, f := range filters {
		if err := t.client.Subscribe(f, byte(defaultQoS), wrappedHandler); err != nil {
			t.logger.Error("subscribe failed", "filter", f, "error", err)
			return err
		}

		t.logger.Debug("subscribed", "filter", f)
	}

	return nil
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
