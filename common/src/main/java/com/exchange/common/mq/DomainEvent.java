package com.exchange.common.mq;

public interface DomainEvent {
    String eventId();
    String aggregateId();
    String eventType();
    long occurredAt();
}
