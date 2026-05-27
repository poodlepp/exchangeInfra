package com.exchange.common.mq.outbox;

import com.exchange.common.mq.DomainEvent;
import com.exchange.common.mq.TransactionalEventPublisher;
import com.exchange.common.mq.kafka.EventEnvelope;
import com.exchange.common.mq.serializer.EventSerializer;
import com.exchange.common.util.SnowflakeIdGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class TransactionalEventPublisherImpl implements TransactionalEventPublisher {

    private final OutboxMapper outboxMapper;
    private final EventSerializer serializer;

    @Override
    public void publish(DomainEvent event) {
        publish(event.eventType(), event);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(String topic, DomainEvent event) {
        OutboxEntity entity = new OutboxEntity();
        entity.setId(SnowflakeIdGenerator.nextDefaultId());
        entity.setEventId(event.eventId());
        entity.setTopic(topic);
        entity.setPartitionKey(event.aggregateId());
        entity.setPayload(serializer.toJson(EventEnvelope.wrap(event)));
        entity.setStatus(OutboxStatus.PENDING.code);
        entity.setRetryCount(0);
        entity.setCreatedAt(LocalDateTime.now());
        outboxMapper.insert(entity);
    }
}
