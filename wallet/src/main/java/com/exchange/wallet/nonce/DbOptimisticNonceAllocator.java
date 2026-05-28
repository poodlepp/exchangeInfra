package com.exchange.wallet.nonce;

import com.exchange.wallet.chain.api.Chain;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * DB 乐观锁实现的并发 nonce 发号器。
 *
 * <p>事务策略：每次 {@link #allocate} 用 {@code REQUIRES_NEW} 独立短事务，
 * 避免嵌入业务事务持锁过久导致并发线程 CAS 持续失败。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DbOptimisticNonceAllocator implements NonceAllocator {

    private static final int MAX_RETRY = 5;

    private final NonceRegisterMapper mapper;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long allocate(Chain chain, String address) {
        String chainName = chain.name();
        for (int i = 0; i < MAX_RETRY; i++) {
            NonceRegisterEntity row = mapper.find(chainName, address);
            if (row == null) {
                throw new IllegalStateException(
                        "nonce_register not initialized for " + chain + "/" + address
                                + " — call reconcile() first");
            }
            long allocated = row.getNextNonce();
            int affected = mapper.casIncrement(chainName, address, row.getVersion());
            if (affected == 1) {
                return allocated;
            }
            log.debug("nonce CAS retry chain={} addr={} attempt={}", chain, address, i + 1);
        }
        throw new IllegalStateException(
                "nonce CAS failed after " + MAX_RETRY + " retries: " + chain + "/" + address);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void reconcile(Chain chain, String address, long onChainPendingNonce) {
        String chainName = chain.name();
        LocalDateTime now = LocalDateTime.now();
        NonceRegisterEntity row = mapper.find(chainName, address);
        if (row == null) {
            mapper.insertIfAbsent(chainName, address, onChainPendingNonce, onChainPendingNonce, now);
            return;
        }
        if (row.getNextNonce() < onChainPendingNonce) {
            int affected = mapper.reconcile(chainName, address, onChainPendingNonce,
                    onChainPendingNonce, now, row.getVersion());
            if (affected == 0) {
                log.warn("nonce reconcile lost CAS race chain={} addr={}", chain, address);
            }
        }
    }
}
