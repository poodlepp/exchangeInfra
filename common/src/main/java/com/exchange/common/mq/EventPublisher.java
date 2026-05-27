package com.exchange.common.mq;

public interface EventPublisher {
    void publish(DomainEvent event);
    void publish(String topic, DomainEvent event);
}
