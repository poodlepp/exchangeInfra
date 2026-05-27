# MQ Outbox 模式

## 一、为什么需要 Outbox

### 题面
业务变更要发 Kafka 通知下游，怎么保证"DB 变更"和"Kafka 投递"原子？

### 要点
- 反例 1：先发 Kafka 后写 DB → DB 失败但事件已发 → 下游凭幻象事件做反应。
- 反例 2：先写 DB 后发 Kafka → DB 成功但 Kafka 失败/超时 → 下游错过事件、对账不齐。
- XA 跨堆栈不可用，性能也差；要换思路。
- **Outbox 模式**：业务变更 + outbox 表 INSERT 同事务 commit；后台 Relay 异步把 outbox 行搬到 Kafka，**at-least-once**。下游必须自己幂等。
- 与之配对的"消费侧 exactly-once 等价"：consumed_record + IdempotentEventHandler。

### 项目中
- `common.mq.outbox.OutboxEntity` / `OutboxRelay` / `TransactionalEventPublisherImpl`
- `Propagation.MANDATORY` 强制调用方先有事务。

### 延伸追问
- 为什么不上 Debezium CDC？— 引入额外组件，对当前规模不划算；轮询表 + 200ms-1s 延迟可接受。
- outbox 行积压怎么办？— PENDING 行年龄 > N 分钟告警；定期归档 SENT 行。

---

## 二、Propagation.MANDATORY 的取舍

### 题面
为什么 `TransactionalEventPublisher.publish` 用 `Propagation.MANDATORY` 而不是 `REQUIRED` / `REQUIRES_NEW`？

### 要点
- **REQUIRED**（默认）：没有事务就开新事务。Publisher 单独事务 → outbox commit 后业务事务回滚 → 出现"业务没成但事件已送达"的幻象。**严重 bug**。
- **REQUIRES_NEW**：永远开新事务，更糟，业务和 outbox 完全异构事务。
- **MANDATORY**：必须有调用方事务，否则抛 `IllegalTransactionStateException`。把 bug 前置到调用方编写阶段，第一次错误调用就直接炸。

### 项目中
`common.mq.outbox.TransactionalEventPublisherImpl#publish` 上 `@Transactional(propagation = Propagation.MANDATORY)`。

### 延伸追问
- 测试集成时注意 `@Transactional` 默认回滚 → outbox 行不可见。要用 `TransactionTemplate.executeWithoutResult` 显式 commit。

---

## 三、OutboxRelay 的几个数值

### 题面
`fixedDelay=1000`、`lockAtMostFor=30s`、`lockAtLeastFor=500ms`、批 200 是怎么来的？

### 要点
- **fixedDelay=1000ms**：上一轮结束到下一轮开始，慢于 1s 时自然背压不堆任务；1s 延迟在交易场景可接受。
- **批 200**：单轮发 200 条平衡吞吐与锁占用时长。
- **lockAtMostFor=30s**：锁最多持有 30s，防止 Relay 崩溃后锁卡死。值要 ≥ 一轮处理的最大耗时（200 条 \* 100ms = 20s），留 50% 余量。
- **lockAtLeastFor=500ms**：即使任务很快结束也不立刻释放，防止"分布式时钟轻微偏差导致同行被两个实例拿到"——A 100ms 跑完释放锁，B 时钟稍快立刻抢锁 + 看到了 A 还没 commit 的更新 = 重发。500ms 是 DB 主从复制延迟的安全阈值。

### 项目中
`OutboxRelay.relay()` 上 `@Scheduled(fixedDelay = 1000) @SchedulerLock(name = "outbox-relay", lockAtMostFor = "30s", lockAtLeastFor = "500ms")`。

### 延伸追问
- 失败指数退避 `min(60 * 2^min(retry, 8), 600)`：retry=0→60s, retry=1→120s, retry≥4→封顶 600s。8 次后不再放大。
- 用 `.get()` 同步阻塞而非 `.thenAccept()`：批内顺序处理简化错误归因。要更高吞吐就开多 Relay 实例 + ShedLock 调度，不在单实例内并发。

---

## 四、消费侧幂等

### 题面
Kafka 默认 at-least-once，下游怎么保证业务逻辑只跑一次？

### 要点
- **`(event_id, handler_name)` 复合主键** 的 `consumed_record` 表。
- IdempotentEventHandler 三段式：
  1. `exists?` 快速路径；
  2. `handle` 业务逻辑；
  3. `markConsumed` 只有 handle 成功才标记。
- **顺序很重要**：先 mark 后 handle 有"丢消息"的风险。
- **`INSERT IGNORE`** 而非 `ON DUPLICATE KEY UPDATE`：第一次写入即定型，零开销。
- 不用 Redis 做去重——持久性弱，掉一次数据就可能重复处理账户事件。

### 项目中
- `common.mq.consumed.ConsumedRecordStore` 提供 `exists` / `markConsumed`
- `common.mq.IdempotentEventHandler` 抽象基类。

### 延伸追问
- 为什么必须 `handle + markConsumed` 同一事务？— 否则 handle 完 mark 失败 → 下次又跑一次（但 handle 内可能有不可逆副作用）。
- handler 内部用 `@Transactional(REQUIRES_NEW)` 会怎样？— mark 不在外层事务里，业务回滚但 mark 已 commit → 下次跳过 → 丢消息。**禁止**。

---

## 五、异常二分：Retriable vs DLT

### 题面
消费异常是该重试还是该送 DLQ？

### 要点
- **`RetriableException`**：transient 失败（DB 死锁、外部接口超时）。原样抛给 spring-kafka 的 `DefaultErrorHandler`，按退避策略重试。
- **其他 `RuntimeException`**：non-retriable（数据格式错、业务校验失败）。原样抛 → `DefaultErrorHandler` 按 `maxAttempts` 用尽后转 DLT topic 等待人工。
- 二分法的好处：业务代码不需要写"是否要重试"的复杂判断，只需决定异常的类型。
- DLQ 路由不在 Handler 类内——交给 spring-kafka 基础设施配置。

### 项目中
`IdempotentEventHandler#onMessage` 内 `catch (RetriableException e) { throw e; }` 透传，`catch (RuntimeException e) { log.error + throw e; }` 同样透传，DLT 路由在 Listener 工厂上配置。

### 延伸追问
- 把 `IllegalArgumentException`（数据格式错）当 `RetriableException` 抛会怎样？— 永远重试到 DLT，浪费 broker 资源。

---

## 六、Producer 的三件套配置

### 题面
为什么 `enable.idempotence=true` + `acks=all` + `max.in.flight.requests.per.connection=5`？

### 要点
- **`enable.idempotence=true`**：单 Producer 实例内防重发（网络重试导致 broker 端重复消息）。
- **`acks=all`**：所有 ISR 写入才算成功，配合 broker 端 `min.insync.replicas≥2` 才能在单节点宕机不丢。
- **`max.in.flight=5`**：开 idempotence 后必须 ≤5（Kafka 限制）；同时保证同分区有序——broker 用 sequence number 重排，应用层无需管乱序。

### 项目中
`common.mq.kafka.KafkaConfig#producerFactory` 写死这三件。

### 延伸追问
- 没开 idempotence + max.in.flight=5 会怎样？— 网络重试可能让先发的 retry 回来比后发的晚到达，分区内乱序。

---

## 七、EventEnvelope 与 schemaVersion

### 题面
事件 payload 演进字段了，怎么不让下游崩？

### 要点
- 事件外壳 `EventEnvelope` = `(schemaVersion, eventType, eventId, occurredAt, aggregateId, payload)`。
- 早期 v1 字段不变；v2 加新字段时下游能根据 `schemaVersion` 走老或新逻辑，避免反序列化失败。
- partition key 用 `aggregateId`：同聚合事件保证分区内有序。

### 项目中
`common.mq.kafka.EventEnvelope.wrap(DomainEvent)` + Jackson 序列化。

### 延伸追问
- 为什么不直接用 Avro / Protobuf 加 Schema Registry？— 当前规模 JSON + schemaVersion 字段够用，少一层基础设施。
