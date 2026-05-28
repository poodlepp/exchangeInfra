package com.exchange.wallet.config;

import org.junit.jupiter.api.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WalletAutoConfiguration 契约测试。
 *
 * 不真起 ApplicationContext —— @MapperScan 会触发 DataSource 自动装配，单测里给不了真实 DB；
 * 实际装配能力由 LedgerServiceImplIT（带 Testcontainers MySQL）覆盖。
 *
 * 这里只校验：
 * 1) AutoConfiguration.imports 文件存在且指向 WalletAutoConfiguration（让 SpringBoot 起飞时能扫到）
 * 2) @ConditionalOnProperty 有 exchange.wallet.enabled 默认开关
 * 3) @ComponentScan / @MapperScan 列出了 Plan 1 范围内的子包，未污染老代码（wallet.controller / wallet.service / wallet.mapper）
 */
class WalletAutoConfigurationTest {

    private static final String IMPORTS_PATH =
            "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports";

    @Test
    void imports_file_lists_wallet_autoconfiguration() throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(IMPORTS_PATH)) {
            assertThat(in).as("AutoConfiguration.imports must exist on classpath").isNotNull();
            String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(body).contains("com.exchange.wallet.config.WalletAutoConfiguration");
        }
    }

    @Test
    void conditional_on_property_with_match_if_missing_true() {
        ConditionalOnProperty cond = WalletAutoConfiguration.class.getAnnotation(ConditionalOnProperty.class);
        assertThat(cond).isNotNull();
        assertThat(cond.name()).containsExactly("exchange.wallet.enabled");
        assertThat(cond.havingValue()).isEqualTo("true");
        assertThat(cond.matchIfMissing()).isTrue();
    }

    @Test
    void component_scan_covers_plan1_subpackages_only() {
        ComponentScan scan = WalletAutoConfiguration.class.getAnnotation(ComponentScan.class);
        assertThat(scan).isNotNull();
        List<String> bases = Arrays.asList(scan.basePackages());
        assertThat(bases).contains(
                "com.exchange.wallet.signer",
                "com.exchange.wallet.core.ledger",
                "com.exchange.wallet.core.address",
                "com.exchange.wallet.nonce",
                "com.exchange.wallet.fee");
        // 红线：不能扫旧业务子包，否则会拖入它们的 controller/service/mapper 依赖
        assertThat(bases)
                .doesNotContain("com.exchange.wallet.controller",
                        "com.exchange.wallet.service",
                        "com.exchange.wallet");
    }

    @Test
    void mapper_scan_lists_only_plan1_mappers() {
        MapperScan scan = WalletAutoConfiguration.class.getAnnotation(MapperScan.class);
        assertThat(scan).isNotNull();
        List<String> bases = Arrays.asList(scan.basePackages());
        assertThat(bases).containsExactlyInAnyOrder(
                "com.exchange.wallet.signer",
                "com.exchange.wallet.core.mapper",
                "com.exchange.wallet.nonce");
        // 老 mapper 子包刻意排除——不属于 Plan 1 链路
        assertThat(bases).doesNotContain("com.exchange.wallet.mapper");
    }
}
