package com.exchange.wallet.config;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Wallet 模块自动装配入口。
 *
 * <p>装配范围：
 * <ul>
 *   <li>{@code wallet.signer} —— AES-GCM / KMS / BIP39+32 / Signer 路由</li>
 *   <li>{@code wallet.core.ledger} —— 双账法账本</li>
 *   <li>{@code wallet.core.address} —— HD 地址池</li>
 *   <li>{@code wallet.nonce} —— EVM nonce 分配</li>
 *   <li>{@code wallet.fee} —— 手续费策略 Registry</li>
 *   <li>{@code wallet.chain.api} —— 链抽象 SPI（接口/DTO，无 Bean，仅供扫到 record/enum）</li>
 * </ul>
 *
 * <p>有意排除：
 * <ul>
 *   <li>{@code wallet.controller / wallet.service} —— 老代码遗留，不在 Plan 1 范围</li>
 *   <li>{@code wallet.chain.btc / eth / tron} —— 由各链 Plan 自带子配置装配</li>
 * </ul>
 *
 * <p>{@code @MapperScan} 列子包而非整 wallet，避免把老 mapper（{@code wallet.mapper}）也扫进来——
 * 该子包属于早期业务代码，不是 Plan 1 链路一部分。
 */
@Configuration
@ConditionalOnProperty(name = "exchange.wallet.enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan(basePackages = {
        "com.exchange.wallet.signer",
        "com.exchange.wallet.core.ledger",
        "com.exchange.wallet.core.address",
        "com.exchange.wallet.nonce",
        "com.exchange.wallet.fee"
})
@MapperScan(basePackages = {
        "com.exchange.wallet.signer",
        "com.exchange.wallet.core.mapper",
        "com.exchange.wallet.nonce"
})
public class WalletAutoConfiguration {
}
