# 本地基础设施 (infra)

本目录为 `exchangeInfra` 项目的**本地开发基础设施编排**,使用 Docker Compose 提供 MySQL、Redis、Kafka(KRaft 模式,无 Zookeeper),以及可选的可观测/管理工具(Kafka UI、Adminer、Redis Commander)。

## 1. 快速启动

```bash
# 1) 复制环境变量文件(可按需修改端口/密码)
cp .env.example .env

# 2) 启动核心三件:MySQL + Redis + Kafka
docker compose up -d

# 3) 同时启动可选管理工具(Kafka UI / Adminer / Redis Commander)
docker compose --profile tools up -d
```

首次启动会拉取镜像并初始化数据卷,Kafka 因 KRaft 元数据初始化耗时略长(20~40s),属正常现象。

## 2. 连接信息

| 组件 | 主机端访问 | 容器间访问 | 账户 / 备注 |
| --- | --- | --- | --- |
| MySQL | `localhost:3306` | `mysql:3306` | `root` / `root`,预创建库:`exchange` / `exchange_dev` / `exchange_test`(均 utf8mb4) |
| Redis | `localhost:6379` | `redis:6379` | 无密码,AOF + RDB 双持久化 |
| Kafka (EXTERNAL) | `localhost:29092` | — | 应用**运行在宿主机**时使用此地址 |
| Kafka (INTERNAL) | — | `kafka:9092` | 应用**运行在容器**且加入同一网络时使用此地址 |

> 关键提示:Spring Boot 应用直接 `mvn spring-boot:run` 跑在宿主机上时,Kafka 连接串必须用 `localhost:29092`。`application-dev.yml` 中的 `spring.kafka.bootstrap-servers` 默认值已对齐此地址。

## 3. 可观测 / 管理工具(profile=tools)

仅在执行 `docker compose --profile tools up -d` 时启动。

| 工具 | 地址 | 登录 |
| --- | --- | --- |
| Kafka UI | http://localhost:8090 | 自动连接 `kafka:9092` |
| Adminer | http://localhost:8091 | System=MySQL,Server=`mysql`,User=`root`,Password=`root` |
| Redis Commander | http://localhost:8092 | 自动连接 `local:redis:6379` |

## 4. 常用命令

```bash
# 查看容器状态
docker compose ps

# 跟踪某个服务日志
docker compose logs -f kafka
docker compose logs -f mysql

# 重启单个服务
docker compose restart redis

# 仅停止(保留数据卷)
docker compose stop

# 完全清除并删除数据卷(谨慎,不可恢复)
docker compose down -v
```

## 5. 与 Spring Boot 应用的联动

`bootstrap/src/main/resources/application-dev.yml` 中的连接默认值已对齐本编排:

- `MYSQL_URL` 默认 `jdbc:mysql://localhost:3306/exchange_dev?...`
- `REDIS_HOST` 默认 `localhost`,`REDIS_PORT` 默认 `6379`,密码为空
- `KAFKA_BOOTSTRAP` 默认 `localhost:29092`

启动顺序:

```bash
# 在 infra/ 目录启动基础设施
docker compose up -d

# 等待健康检查通过(可 docker compose ps 查看 STATUS = healthy)
# 切到 bootstrap 启动应用
cd ../bootstrap
mvn spring-boot:run
```

## 6. 常见问题

- **macOS 上 3306 端口被本地 MySQL 占用** → 编辑 `.env`,把 `MYSQL_PORT` 改为 3307 之类,然后同步修改应用侧 `MYSQL_URL`。
- **Kafka 启动慢 / `service_healthy` 一直没绿** → 属正常,KRaft 初次启动需要生成元数据。耐心等约 30~40 秒;若超过 2 分钟仍异常,看 `docker compose logs kafka`。
- **重置 Kafka 卷会丢失全部 topic** → `docker compose down -v` 会清掉所有 broker 元数据和消息。生产环境永远不要这么干,本地开发想干净重启时可用。
- **`lower_case_table_names=1` 想生效** → 必须在 MySQL 数据卷为空时初始化,即先 `docker compose down -v` 再 `up -d`。
- **改了 `my.cnf` 不生效** → `docker compose restart mysql` 让其重新加载;若改的是 `lower_case_table_names`,需 down -v 重置。
