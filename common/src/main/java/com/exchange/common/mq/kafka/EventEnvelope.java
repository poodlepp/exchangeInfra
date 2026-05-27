package com.exchange.common.mq.kafka;

import com.exchange.common.mq.DomainEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope {
    private int schemaVersion;
    private String eventType;
    private String eventId;
    private long occurredAt;
    private String aggregateId;
    private Object payload;

    public static EventEnvelope wrap(DomainEvent event) {
        return new EventEnvelope(1, event.eventType(), event.eventId(),
                event.occurredAt(), event.aggregateId(), event);
    }
}
