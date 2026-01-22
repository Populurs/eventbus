package eventbus

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventBus 接口定义
type EventBus interface {
	Publish(ctx context.Context, topic string, payload []byte, metadata map[string]string) error
	Subscribe(ctx context.Context, topic string, handler func(*message.Message)) error
}

// RabbitMQEventBus 实现
type RabbitMQEventBus struct {
	publisher  *amqp.Publisher
	subscriber *amqp.Subscriber
}

// NewRabbitMQEventBus 初始化事件总线
func NewRabbitMQEventBus(amqpUri string) (EventBus, error) {

	config := amqp.NewDurableQueueConfig(amqpUri)
	logger := watermill.NewStdLogger(false, true)

	// 创建发布器
	publisher, err := amqp.NewPublisher(
		config,
		logger,
	)
	if err != nil {
		return nil, err
	}

	// 创建订阅器
	subscriber, err := amqp.NewSubscriber(
		config,
		logger,
	)
	if err != nil {
		return nil, err
	}

	return &RabbitMQEventBus{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

// Publish 发布事件
func (e *RabbitMQEventBus) Publish(ctx context.Context, topic string, payload []byte, metadata map[string]string) error {
	msg := message.NewMessage(
		watermill.NewUUID(),
		payload,
	)
	msg.SetContext(ctx)
	msg.Metadata = metadata
	msg.Metadata.Set("event_type", topic)

	return e.publisher.Publish(topic, msg)
}

// Subscribe 订阅事件
func (e *RabbitMQEventBus) Subscribe(ctx context.Context, topic string, handler func(*message.Message)) error {
	msgs, err := e.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			handler(msg)
			msg.Ack()
		}
	}()

	return nil
}
