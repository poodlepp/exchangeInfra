package com.exchange.wallet.nonce;

import com.exchange.wallet.chain.api.Chain;

/**
 * 并发安全的 nonce 发号器（适用于 EVM 类链：ETH/TRON）。
 *
 * <p>实现要点：
 * - {@link #allocate} 必须独立短事务，不嵌入业务事务
 * - DB 乐观锁 CAS 是真理之源；冲突时自旋重试
 * - {@link #reconcile} 启动期 / 5min 巡检调用，把 next_nonce 校准到链上 pending nonce
 */
public interface NonceAllocator {

    /**
     * 为 (chain, address) 发一个新的 nonce 值（递增）。
     *
     * @param chain 链类型
     * @param address 出账地址
     * @return 分配的 nonce（即调用前的 next_nonce 值）
     * @throws IllegalStateException 重试上限仍 CAS 失败 / 行未初始化
     */
    long allocate(Chain chain, String address);

    /**
     * 用链上 pending nonce 校准本地 next_nonce。
     *
     * <p>启动期首次接入地址 / 定时巡检调用：
     * - 行不存在 → 初始化为 onChainPendingNonce
     * - 行存在 → 若 next_nonce &lt; onChainPendingNonce，更新为 onChainPendingNonce
     */
    void reconcile(Chain chain, String address, long onChainPendingNonce);
}
