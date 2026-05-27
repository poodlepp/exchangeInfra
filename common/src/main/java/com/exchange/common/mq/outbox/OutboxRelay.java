package com.exchange.common.mq.outbox;

import com.exchange.common.mq.serializer.EventSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private static final int BATCH = 200;

    private final OutboxMapper mapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventSerializer serializer;

    @Scheduled(fixedDelay = 1000)
    @SchedulerLock(name = "outbox-relay", lockAtMostFor = "30s", lockAtLeastFor = "500ms")
    public void relay() {
        List<OutboxEntity> rows = mapper.pickPending(
                OutboxStatus.PENDING.code, LocalDateTime.now(), BATCH);
        for (OutboxEntity row : rows) sendOne(row);
    }

    private void sendOne(OutboxEntity row) {
        try {
            kafkaTemplate.send(row.getTopic(), row.getPartitionKey(), row.getPayload())
                         .get();
            mapper.markStatus(row.getId(), OutboxStatus.SENT.code);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | RuntimeException e) {
            int next = row.getRetryCount() + 1;
            long delaySec = Math.min(60L * (1L << Math.min(next, 8)), 600L);
            mapper.markRetry(row.getId(), LocalDateTime.now().plusSeconds(delaySec));
            log.warn("outbox send failed id={} retry={} cause={}", row.getId(), next, e.toString());
        }
    }
}
