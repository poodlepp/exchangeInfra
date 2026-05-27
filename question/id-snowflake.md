# 雪花算法（Snowflake）

## 一、原理 & 位段划分

### 题面
雪花算法 64-bit 怎么切？为什么这样切？

### 要点
```
| 1 bit 符号 | 41 bit timestamp | 5 bit datacenterId | 5 bit workerId | 12 bit sequence |
```
- **1 bit 符号位**：恒 0，保证 long 为正。
- **41 bit 时间戳**：相对自定义 EPOCH 的毫秒差，可用约 **69 年**（`2^41 / (365*24*3600*1000)`）。
- **5+5 bit 机器位**：共 1024 个节点。
- **12 bit 序列号**：单机单毫秒上限 `2^12 = 4096`，约 **400w QPS / 节点**。
- 拼装：各段左移到自己的槽位再 `|` 起来——位移量 = 后面所有段的位宽和，互不重叠。

### 项目中
`common/src/main/java/com/exchange/common/util/SnowflakeIdGenerator.java`：`EPOCH = 1735689600000L`（2025-01-01），位宽与上表一致；`nextId()` 第 78~81 行就是位移 + 或拼装。

---

## 二、时钟回拨

### 题面
NTP 让系统时间倒退了 3ms，怎么办？倒退 1 分钟呢？

### 要点
- **必须解决**：时间戳是高位，回拨 → 同 (worker, seq) 可能撞号、且打破单调递增。
- 常见策略（按"从轻到重"）：
  1. **小幅回拨自旋等待**：`offset ≤ 阈值` 时 `wait(2*offset)` 后重试。
  2. **大幅回拨拒绝发号**：抛异常让上游降级，绝不发出非递增 ID。
  3. **持久化 lastTimestamp**：ZK / Redis / 本地文件，跨重启续，启动时 `max(now, lastTs)`。
  4. **借未来时间**：百度 UidGenerator 思路——预生成未来时间段的 ID 池，完全规避回拨。
  5. **运维兜底**：NTP 配 `slew` 模式（缓慢校正）而非 `step`（瞬时跳变）。

### 项目中
`SnowflakeIdGenerator.java:50-65`：`offset ≤ 5ms` 自我 `wait(offset<<1)` 重取一次；超过 5ms 直接 `IllegalStateException`。**没有持久化 lastTimestamp**——单机重启理论上仍可能撞号，生产建议补持久化或换 UidGenerator。

### 延伸追问
- 为什么不无限等待？大幅回拨等下去也无意义，业务上更愿意"快速失败 → 上游降级/重试"。
- 为什么阈值取 5ms？经验值，覆盖正常 NTP 抖动；超过 5ms 多半是异常时间跳变。

---

## 三、workerId / datacenterId 分配

### 题面
1024 个机器位怎么分？容器化多副本怎么保证不撞？

### 要点
- **静态配置**：配置文件里写死（最稳，但运维负担大）。
- **启动时去 ZK 注册临时顺序节点**：拿到的序号即 workerId，节点掉线自动释放。
- **Redis INCR**：`INCR worker:seq` 取模 1024，简单但需考虑 Redis 持久化。
- **MAC + PID 兜底 hash**：best-effort，**不能保证唯一**——同主机多容器、相同 MAC 的网卡都会撞。
- 一旦撞号 = 同毫秒同 sequence 重复发号，**无法事后修复**，必须前置防住。

### 项目中
`SnowflakeIdGenerator.defaultWorkerId()`：`MAC ^ PID ^ JVMName.hashCode() % 32`，仅作为单元测试 / 无 Spring 上下文场景的兜底。生产路径强制注入 Bean，`workerId/datacenterId` 由 `exchange.snowflake.worker-id` / `exchange.snowflake.datacenter-id` 配置。

### 延伸追问
- K8s StatefulSet 顺序 ID 能直接当 workerId 吗？可以，但要注意 ordinal ≥ 32 时溢出。
- ZK 节点掉线重连，新分配的 workerId 复用了旧的怎么办？要等 lastTimestamp 推进过宕机时段（持久化 + 启动等待）。

---

## 四、单机线程安全 & 性能

### 题面
`synchronized nextId()` 会不会成瓶颈？

### 要点
- 临界区只有位运算 + `System.currentTimeMillis()`，**无 IO**，单 JVM 几百万 QPS 不是问题。
- 真要榨性能可以做：
  - 改 `LongAdder` 思路，每线程一个 sequence 区段；
  - 或拆成多 generator，按线程哈希路由（牺牲全局单调性换吞吐）。
- **不要用 ReentrantLock 替换** synchronized：JIT 已对 synchronized 有重度优化，无锁竞争时几乎零开销。

### 项目中
直接 `synchronized`，配合 `lastTimestamp` / `sequence` 两个实例变量。够用。

---

## 五、与其他 ID 方案的对比

### 题面
为什么不用 UUID / 数据库自增 / Redis INCR？

### 要点
| 方案 | 优点 | 致命缺点 |
|---|---|---|
| UUID | 无中心化 | 128 bit、字符串、**无序** → 索引页频繁分裂 |
| DB 自增 | 单调 | 单点瓶颈、扩展难、暴露业务量 |
| Redis INCR | 单调、性能好 | 强依赖 Redis 可用性、需持久化 |
| Snowflake | long 8 字节、**趋势递增**、本地生成无 RTT | 时钟回拨问题、机器位需协调分配 |

雪花的"趋势递增"对 InnoDB B+ 树主键尤其友好——追加写入避免页分裂，`primary key` 即聚簇索引的天然选择。

### 延伸追问
- 为什么是"趋势递增"而非"严格递增"？多机并发下不同节点同毫秒序列号穿插，整体趋势单调但局部可能逆序。
- 业务上需要严格递增怎么办？单点发号 + 持久化（如 Leaf-segment）。

---

## 六、EPOCH 选取

### 题面
EPOCH 为什么要自定义，不能用 1970-01-01？

### 要点
- 41 bit 容量是**从 EPOCH 开始算 69 年**。
- 用 1970 → 已"消耗"50+ 年，剩不到 20 年。
- 用项目"上线日期附近的某个整时刻" → 满血 69 年。
- **一旦定下不能改**：改 EPOCH 等同于换 ID 命名空间，老数据可能与新 ID 撞号。

### 项目中
`EPOCH = 1735689600000L`（2025-01-01 00:00:00 UTC），可用至 ~2094 年。
