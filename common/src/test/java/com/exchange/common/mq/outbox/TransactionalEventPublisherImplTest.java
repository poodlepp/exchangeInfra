package com.exchange.common.mq.outbox;

import com.exchange.common.mq.AbstractDomainEvent;
import com.exchange.common.mq.serializer.EventSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TransactionalEventPublisherImplTest {

    static class FooEvent extends AbstractDomainEvent {
        @Override public String aggregateId() { return "agg-1"; }
        @Override public String eventType() { return "test.foo"; }
    }

    @Test
    void publish_writes_pending_outbox_row() {
        OutboxMapper mapper = mock(OutboxMapper.class);
        EventSerializer serializer = mock(EventSerializer.class);
        when(serializer.toJson(any())).thenReturn("{}");

        TransactionalEventPublisherImpl pub =
                new TransactionalEventPublisherImpl(mapper, serializer);
        FooEvent e = new FooEvent();

        pub.publish(e);

        ArgumentCaptor<OutboxEntity> cap = ArgumentCaptor.forClass(OutboxEntity.class);
        verify(mapper).insert(cap.capture());
        OutboxEntity row = cap.getValue();
        assertThat(row.getEventId()).isEqualTo(e.eventId());
        assertThat(row.getTopic()).isEqualTo("test.foo");
        assertThat(row.getPartitionKey()).isEqualTo("agg-1");
        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING.code);
        assertThat(row.getRetryCount()).isZero();
    }
}
