RabbitMQ 事件总线组件
组件介绍
这是一个基于 RabbitMQ 的事件总线实现，提供了简单的发布/订阅功能，使用 Watermill 库作为底层 AMQP 协议实现。

主要特性
接口抽象：定义了通用的 EventBus 接口，便于替换不同的消息中间件实现
发布订阅：支持消息的发布和订阅功能
元数据支持：在消息中携带额外的元数据信息
自动确认：订阅者处理完消息后自动发送 ACK 确认
核心组件
EventBus：事件总线接口定义
RabbitMQEventBus：RabbitMQ 实现的具体结构体
NewRabbitMQEventBus：创建事件总线实例的工厂函数
Event Bus
基于 RabbitMQ 的事件总线实现，使用 Watermill 库提供 AMQP 支持。

功能特性
消息发布/订阅模式
支持消息元数据
自动消息确认机制
接口化设计，易于扩展
安装
bash
go get github.com/your-repo/eventbus
使用示例
go
package main

import (
    "context"
    "eventbus"
    "github.com/ThreeDotsLabs/watermill/message"
)

func main() {
    // 创建事件总线实例
    eb, err := eventbus.NewRabbitMQEventBus("amqp://user:password@localhost:5672/")
    if err != nil {
        panic(err)
    }

    // 订阅主题
    err = eb.Subscribe(context.Background(), "test-topic", func(msg *message.Message) {
        println("Received:", string(msg.Payload))
    })
    if err != nil {
        panic(err)
    }

    // 发布消息
    err = eb.Publish(context.Background(), "test-topic", []byte("Hello World"), nil)
    if err != nil {
        panic(err)
    }
}
API 接口
Publish(ctx, topic, payload, metadata) - 发布消息到指定主题
Subscribe(ctx, topic, handler) - 订阅指定主题的消息
依赖
github.com/ThreeDotsLabs/watermill
github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp
许可证
MIT
