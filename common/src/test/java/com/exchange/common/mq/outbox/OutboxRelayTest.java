package com.exchange.common.mq.outbox;

import com.exchange.common.mq.serializer.EventSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class OutboxRelayTest {

    @Test
    @SuppressWarnings("unchecked")
    void successful_send_marks_sent() {
        OutboxMapper mapper = mock(OutboxMapper.class);
        KafkaTemplate<String, String> tpl = mock(KafkaTemplate.class);
        EventSerializer ser = mock(EventSerializer.class);

        OutboxEntity row = new OutboxEntity();
        row.setId(1L);
        row.setTopic("t");
        row.setPartitionKey("k");
        row.setPayload("{}");
        row.setRetryCount(0);
        row.setStatus(OutboxStatus.PENDING.code);
        when(mapper.pickPending(eq(0), any(LocalDateTime.class), eq(200)))
                .thenReturn(List.of(row));

        SendResult<String, String> res = mock(SendResult.class);
        RecordMetadata md = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0L, 0, 0);
        when(res.getRecordMetadata()).thenReturn(md);
        when(tpl.send("t", "k", "{}")).thenReturn(CompletableFuture.completedFuture(res));

        new OutboxRelay(mapper, tpl, ser).relay();

        verify(mapper).markStatus(1L, OutboxStatus.SENT.code);
        verify(mapper, never()).markRetry(anyLong(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void failed_send_marks_retry_with_backoff() {
        OutboxMapper mapper = mock(OutboxMapper.class);
        KafkaTemplate<String, String> tpl = mock(KafkaTemplate.class);
        EventSerializer ser = mock(EventSerializer.class);

        OutboxEntity row = new OutboxEntity();
        row.setId(2L);
        row.setTopic("t");
        row.setPartitionKey("k");
        row.setPayload("{}");
        row.setRetryCount(0);
        row.setStatus(OutboxStatus.PENDING.code);
        when(mapper.pickPending(eq(0), any(LocalDateTime.class), eq(200)))
                .thenReturn(List.of(row));

        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(tpl.send("t", "k", "{}")).thenReturn(failed);

        new OutboxRelay(mapper, tpl, ser).relay();

        verify(mapper, never()).markStatus(anyLong(), anyInt());
        verify(mapper).markRetry(eq(2L), any(LocalDateTime.class));
    }
}
