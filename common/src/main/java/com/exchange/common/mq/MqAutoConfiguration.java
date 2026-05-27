package com.exchange.common.mq;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "exchange.mq.enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan(basePackages = "com.exchange.common.mq")
@MapperScan(basePackages = {
        "com.exchange.common.mq.outbox",
        "com.exchange.common.mq.consumed"
})
public class MqAutoConfiguration {
}
