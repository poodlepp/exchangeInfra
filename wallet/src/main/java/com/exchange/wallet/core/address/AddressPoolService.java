package com.exchange.wallet.core.address;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.chain.api.AddressDerivator;
import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.DerivedAddress;
import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.core.entity.HdPathEntity;
import com.exchange.wallet.core.entity.WalletAddressEntity;
import com.exchange.wallet.core.mapper.HdPathMapper;
import com.exchange.wallet.core.mapper.WalletAddressMapper;
import com.exchange.wallet.signer.KeyMaterialService;
import com.exchange.wallet.signer.kms.AesGcmCipher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HD 钱包地址池服务。
 *
 * <p>核心契约：每个 (chain, hd_path) 只能被分配一次——靠 hd_path 表的
 * uk(chain, hd_path) 兜底。并发场景下，两个线程同时声明同一 path 时，第二个会撞 uk
 * 抛 DuplicateKeyException，触发 service 层自旋（递增 index 重试），直到成功。
 *
 * <p>路径策略（与 §6.5 一致）：
 * <ul>
 *   <li>EVM 系链：m/44'/60'/0'/0/{index} （ETH 共用一条派生路径，不同链同址）</li>
 *   <li>BTC：m/44'/0'/0'/0/{index}（按 SLIP-0044）</li>
 *   <li>TRON：m/44'/195'/0'/0/{index}</li>
 * </ul>
 *
 * <p>种子隔离：seed 仅在 allocate 内部出现，调用 derive 后立即 wipe；
 * 私钥/种子不进 entity 字段、不进日志、不进异常。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AddressPoolService {

    private final HdPathMapper hdPathMapper;
    private final WalletAddressMapper walletAddressMapper;
    private final KeyMaterialService keyMaterialService;
    private final List<AddressDerivator> derivators;

    /** chain -> derivator 的运行期路由表（启动期一次性构造，避免每次分配都 stream.collect） */
    private final Map<Chain, AddressDerivator> derivatorByChain = new HashMap<>();
    /** chain -> 下一个尝试的 index 缓存（仅用作起点，撞 uk 后会自旋） */
    private final Map<Chain, AtomicLong> nextIndexHint = new HashMap<>();

    private static final int MAX_ALLOCATE_RETRY = 64;

    @PostConstruct
    void initRoutingTable() {
        for (AddressDerivator d : derivators) {
            AddressDerivator prev = derivatorByChain.put(d.chain(), d);
            if (prev != null) {
                throw new IllegalStateException("duplicate AddressDerivator for chain " + d.chain());
            }
            nextIndexHint.put(d.chain(), new AtomicLong(0));
        }
    }

    /**
     * 为用户在指定链上派生一个未使用过的地址。
     *
     * @param userId  用户 ID
     * @param chain   目标链
     * @param keyId   该用户的 HD 种子 keyId（由 KeyMaterialService.storeHdSeed 提前签发）
     * @return KeyRef，包含 chain / keyId / hdPath / address，可直接喂给 Signer.sign
     */
    @Transactional
    public KeyRef allocate(long userId, Chain chain, String keyId) {
        AddressDerivator derivator = derivatorByChain.get(chain);
        if (derivator == null) {
            throw new IllegalStateException("no AddressDerivator for chain " + chain);
        }

        AtomicLong hint = nextIndexHint.get(chain);
        for (int attempt = 0; attempt < MAX_ALLOCATE_RETRY; attempt++) {
            long index = hint.getAndIncrement();
            String hdPath = buildHdPath(chain, index);

            // Step 1: 占坑 hd_path——抢到 uk(chain, hd_path) 的人胜
            try {
                HdPathEntity pathRow = new HdPathEntity();
                pathRow.setId(SnowflakeIdGenerator.nextDefaultId());
                pathRow.setChain(chain.name());
                pathRow.setHdPath(hdPath);
                pathRow.setUsedAt(LocalDateTime.now());
                hdPathMapper.insert(pathRow);
            } catch (DuplicateKeyException race) {
                // 别人抢了这个 index，下一轮自旋
                log.debug("hd_path race chain={} path={} retrying", chain, hdPath);
                continue;
            }

            // Step 2: 派生公钥 / 地址；种子用完 wipe
            byte[] seed = null;
            DerivedAddress derived;
            try {
                seed = keyMaterialService.loadSeed(keyId);
                derived = derivator.derive(seed, hdPath);
            } finally {
                AesGcmCipher.wipe(seed);
            }

            // Step 3: 落 wallet_address；按 uk(chain, address) 兜底（极端碰撞场景）
            WalletAddressEntity addr = new WalletAddressEntity();
            addr.setId(SnowflakeIdGenerator.nextDefaultId());
            addr.setUserId(userId);
            addr.setChain(chain.name());
            addr.setAddress(derived.getAddress());
            addr.setHdPath(hdPath);
            addr.setKeyId(keyId);
            addr.setStatus(1);
            LocalDateTime now = LocalDateTime.now();
            addr.setCreatedAt(now);
            addr.setUpdatedAt(now);
            walletAddressMapper.insert(addr);

            return KeyRef.builder()
                    .chain(chain)
                    .keyId(keyId)
                    .hdPath(hdPath)
                    .address(derived.getAddress())
                    .build();
        }

        throw new IllegalStateException("exhausted allocate retries for chain=" + chain);
    }

    /** EVM 系 / BTC / TRON 的标准 BIP44 路径。 */
    private static String buildHdPath(Chain chain, long index) {
        int coinType = switch (chain) {
            case ETH -> 60;
            case BTC -> 0;
            case TRON -> 195;
        };
        return "m/44'/" + coinType + "'/0'/0/" + index;
    }
}
