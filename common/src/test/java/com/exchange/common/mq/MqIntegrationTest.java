package com.exchange.common.mq;

import com.exchange.common.mq.outbox.OutboxMapper;
import com.exchange.common.mq.outbox.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = MqTestApplication.class)
@ActiveProfiles("mqtest")
@Testcontainers
@EmbeddedKafka(partitions = 1, topics = {"test.foo"})
class MqIntegrationTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withUsername("test").withPassword("test").withDatabaseName("mq_test")
            // Testcontainers 1.20.x 默认用老 driver com.mysql.jdbc.Driver 探活，
            // 与 mysql-connector-j 8 的 com.mysql.cj.jdbc.Driver 不兼容；改用日志等待。
            .waitingFor(Wait.forLogMessage(".*ready for connections.*\\n", 2)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("mysql.url", mysql::getJdbcUrl);
        r.add("kafka.bootstrap", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Autowired TransactionalEventPublisher txPublisher;
    @Autowired OutboxMapper outboxMapper;
    @Autowired PlatformTransactionManager txManager;

    static class FooEvent extends AbstractDomainEvent {
        @Override public String aggregateId() { return "agg"; }
        @Override public String eventType() { return "test.foo"; }
    }

    @Test
    void publish_lands_pending_then_relay_marks_sent() {
        FooEvent e = new FooEvent();

        // 必须用 TransactionTemplate 显式 commit；不能用 @Transactional（默认回滚）
        new TransactionTemplate(txManager).executeWithoutResult(s -> txPublisher.publish(e));

        // OutboxRelay 每秒轮询一次，10 秒内应被消化为 SENT（pickPending 不再含此 eventId）
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            var rows = outboxMapper.pickPending(OutboxStatus.PENDING.code,
                    LocalDateTime.now().plusYears(1), 100);
            assertThat(rows).noneMatch(r -> r.getEventId().equals(e.eventId()));
        });
    }
}
