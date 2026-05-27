package com.exchange.common.mq.kafka;

import com.exchange.common.mq.DomainEvent;
import com.exchange.common.mq.EventPublisher;
import com.exchange.common.mq.serializer.EventSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventSerializer serializer;

    @Override
    public void publish(DomainEvent event) {
        publish(event.eventType(), event);
    }

    @Override
    public void publish(String topic, DomainEvent event) {
        String json = serializer.toJson(EventEnvelope.wrap(event));
        kafkaTemplate.send(topic, event.aggregateId(), json);
    }
}
