package com.exchange.common.mq;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;

@Getter
public abstract class AbstractDomainEvent implements DomainEvent {
    private final String eventId;
    private final long occurredAt;

    protected AbstractDomainEvent() {
        this.eventId = String.valueOf(SnowflakeIdGenerator.nextDefaultId());
        this.occurredAt = System.currentTimeMillis();
    }

    protected AbstractDomainEvent(String eventId, long occurredAt) {
        this.eventId = eventId;
        this.occurredAt = occurredAt;
    }

    @Override public String eventId() { return eventId; }
    @Override public long occurredAt() { return occurredAt; }
    @JsonIgnore @Override public abstract String aggregateId();
    @JsonIgnore @Override public abstract String eventType();
}
