package eventbus

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventBus 接口定义
type EventBus interface {
	Publish(ctx context.Context, topic string, payload []byte, metadata map[string]string) error
	Subscribe(ctx context.Context, topic string, handler EventHandler) error
}

// RabbitMQEventBus 实现
type RabbitMQEventBus struct {
	publisher     *amqp.Publisher
	subscriber    *amqp.Subscriber
	prefetchCount int
}

type EventHandler func(eventID string, payload []byte, metadata map[string]string) error

// NewRabbitMQEventBus 初始化事件总线
// prefetchCount 控制 RabbitMQ QoS PrefetchCount 及并发消费数，<=0 时默认为 1
func NewRabbitMQEventBus(amqpUri string, prefetchCount int) (EventBus, error) {
	if prefetchCount <= 0 {
		prefetchCount = 1
	}

	config := amqp.NewDurableQueueConfig(amqpUri)
	config.Consume.Qos.PrefetchCount = prefetchCount
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
		publisher:     publisher,
		subscriber:    subscriber,
		prefetchCount: prefetchCount,
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

// Subscribe 订阅事件，并发消费，并发度与 PrefetchCount 一致
func (e *RabbitMQEventBus) Subscribe(ctx context.Context, topic string, handler EventHandler) error {
	msgs, err := e.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	go func() {
		sem := make(chan struct{}, e.prefetchCount)
		var wg sync.WaitGroup
		for msg := range msgs {
			sem <- struct{}{}
			wg.Add(1)
			go func(m *message.Message) {
				defer func() { <-sem; wg.Done() }()
				if err := handler(m.UUID, m.Payload, m.Metadata); err != nil {
					m.Nack()
				} else {
					m.Ack()
				}
			}(msg)
		}
		wg.Wait()
	}()

	return nil
}