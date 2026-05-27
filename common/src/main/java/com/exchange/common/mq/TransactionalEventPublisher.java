package com.exchange.common.mq;

public interface TransactionalEventPublisher {
    void publish(DomainEvent event);
    void publish(String topic, DomainEvent event);
}
