package com.exchange.common.mq;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class AbstractDomainEventTest {

    static class FooEvent extends AbstractDomainEvent {
        private final String aggregateId;
        FooEvent(String aggregateId) { this.aggregateId = aggregateId; }
        @Override public String aggregateId() { return aggregateId; }
        @Override public String eventType() { return "test.foo.created"; }
    }

    @Test
    void event_id_and_occurred_at_are_auto_filled() {
        FooEvent e = new FooEvent("agg-1");
        assertThat(e.eventId()).isNotBlank();
        assertThat(e.occurredAt()).isPositive();
        assertThat(e.aggregateId()).isEqualTo("agg-1");
        assertThat(e.eventType()).isEqualTo("test.foo.created");
    }
}
