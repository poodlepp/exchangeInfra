# exchangeInfra

CEX (中心化交易所) 后端 Java 多模块脚手架。基于 Spring Boot 3.3.5 + Java 21 LTS,Maven 多模块单体架构,适合作为快速搭建交易所后端的起点。

## 技术栈

| 类别 | 选型 | 版本 |
| --- | --- | --- |
| 语言 | Java | 21 LTS |
| 框架 | Spring Boot | 3.3.5 |
| 构建 | Maven | 3.9+ |
| ORM | MyBatis-Plus | 3.5.7 |
| 数据库 | MySQL | 8.x |
| 连接池 | Druid | 1.2.23 |
| 数据库迁移 | Flyway | 10.18.0 |
| 缓存/分布式锁 | Redis + Redisson | 3.34.1 |
| 工具库 | Hutool | 5.8.32 |
| Bean 映射 | MapStruct | 1.6.2 |
| 简化样板 | Lombok | 1.18.34 |
| API 文档 | springdoc-openapi | 2.6.0 |
| 鉴权 | JJWT | 0.12.6 |

## 模块说明

| 模块 | artifactId | 职责 |
| --- | --- | --- |
| common | exchange-common | 通用工具/配置/异常/响应封装、JWT、雪花 ID 等 |
| user | exchange-user | 用户、KYC、登录、权限 |
| trade | exchange-trade | 订单、撮合接入、成交 |
| wallet | exchange-wallet | 钱包、充提、流水、冻结 |
| market | exchange-market | 行情、K 线、Tick |
| risk | exchange-risk | 风控、限额、反洗 |
| admin | exchange-admin | 后台管理 |
| bootstrap | exchange-bootstrap | 启动入口,聚合全部模块 |

模块依赖关系: 业务模块均依赖 `common`;`wallet` / `risk` 依赖 `user`;`trade` 依赖 `user`、`wallet`、`risk`;`admin` 依赖全部业务模块。

## 启动方式

```bash
# 1. 在项目根目录构建
mvn clean install -DskipTests

# 2. 启动 bootstrap 模块
cd bootstrap
mvn spring-boot:run

# 或直接运行打好的可执行 jar
java -jar bootstrap/target/exchange-bootstrap-1.0.0-SNAPSHOT.jar
```

启动成功后:
- 健康检查: http://localhost:8080/actuator/health
- Swagger UI: http://localhost:8080/swagger-ui.html
- 各模块示例: `/user/health`、`/trade/health`、`/wallet/health`、`/market/health`、`/risk/health`、`/admin/health`

## 中间件依赖

| 中间件 | 版本 | 说明 |
| --- | --- | --- |
| MySQL | 8.x | 主数据库,Flyway 自动建表 |
| Redis | 7.x | 缓存、分布式锁(Redisson) |

可通过环境变量覆盖默认连接配置:`MYSQL_URL`、`MYSQL_USER`、`MYSQL_PASSWORD`、`REDIS_HOST`、`REDIS_PORT`、`REDIS_PASSWORD`。

## 目录结构

```
exchangeInfra/
├── pom.xml                  # 父 POM,聚合所有子模块
├── README.md
├── .gitignore
├── frontend/                # 预留给前端工程
│   └── README.md
├── common/                  # 通用基础模块
├── user/                    # 用户域
├── trade/                   # 交易域
├── wallet/                  # 钱包域
├── market/                  # 行情域
├── risk/                    # 风控域
├── admin/                   # 后台管理
└── bootstrap/               # 启动入口
    └── src/main/resources/
        ├── application.yml
        ├── application-dev.yml
        ├── application-prod.yml
        ├── logback-spring.xml
        └── db/migration/    # Flyway SQL 脚本
```

## 前端

`frontend/` 目录预留给前端同事自行初始化(建议 Vite + React/Vue + TypeScript)。详见 [frontend/README.md](./frontend/README.md)。
