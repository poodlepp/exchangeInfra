package com.exchange.wallet.core.ledger;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Ledger 集成测试专用 Spring Boot 启动器：
 * - 只扫 wallet.core.ledger / wallet.core.mapper 这两个子包，避免拉起其他 module 的依赖
 * - 排除 wallet.signer / wallet.config 等需要 KMS / Redis 的子包
 */
@SpringBootApplication(scanBasePackages = {
        "com.exchange.wallet.core.ledger",
        "com.exchange.wallet.core.mapper",
        "com.exchange.common.util"
})
@MapperScan("com.exchange.wallet.core.mapper")
@EnableTransactionManagement
@ComponentScan(
        basePackages = {
                "com.exchange.wallet.core.ledger",
                "com.exchange.wallet.core.mapper"
        },
        useDefaultFilters = false,
        includeFilters = @ComponentScan.Filter(type = FilterType.ANNOTATION,
                classes = {org.springframework.stereotype.Service.class,
                        org.springframework.stereotype.Component.class})
)
public class LedgerTestApplication {
}
