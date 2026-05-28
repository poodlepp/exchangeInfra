package com.exchange.wallet.core.address;

import com.exchange.wallet.chain.api.AddressDerivator;
import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.DerivedAddress;
import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.core.entity.HdPathEntity;
import com.exchange.wallet.core.entity.WalletAddressEntity;
import com.exchange.wallet.core.mapper.HdPathMapper;
import com.exchange.wallet.core.mapper.WalletAddressMapper;
import com.exchange.wallet.signer.KeyMaterialService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AddressPoolService 单元测试 — HD path 唯一性 + 自旋 + 路由错误。
 *
 * 不需要 Docker：纯 Mockito，覆盖关键路径。集成层（uk 真撞）由 IT 在 Plan 2 落地链实现后补。
 */
@ExtendWith(MockitoExtension.class)
class AddressPoolServiceTest {

    @Mock HdPathMapper hdPathMapper;
    @Mock WalletAddressMapper walletAddressMapper;
    @Mock KeyMaterialService keyMaterialService;

    AddressPoolService service;
    AddressDerivator ethDerivator;

    @BeforeEach
    void setUp() {
        ethDerivator = new StubEthDerivator();
        service = new AddressPoolService(
                hdPathMapper, walletAddressMapper, keyMaterialService, List.of(ethDerivator));
        service.initRoutingTable();
    }

    @Test
    void allocate_succeeds_first_try_and_returns_keyref() {
        when(keyMaterialService.loadSeed("k1")).thenReturn(new byte[32]);

        KeyRef ref = service.allocate(1001L, Chain.ETH, "k1");

        assertThat(ref.getChain()).isEqualTo(Chain.ETH);
        assertThat(ref.getKeyId()).isEqualTo("k1");
        assertThat(ref.getHdPath()).isEqualTo("m/44'/60'/0'/0/0");
        assertThat(ref.getAddress()).isEqualTo("0xstub-m/44'/60'/0'/0/0");

        ArgumentCaptor<HdPathEntity> hdCap = ArgumentCaptor.forClass(HdPathEntity.class);
        verify(hdPathMapper).insert(hdCap.capture());
        assertThat(hdCap.getValue().getHdPath()).isEqualTo("m/44'/60'/0'/0/0");
        assertThat(hdCap.getValue().getChain()).isEqualTo("ETH");

        ArgumentCaptor<WalletAddressEntity> addrCap = ArgumentCaptor.forClass(WalletAddressEntity.class);
        verify(walletAddressMapper).insert(addrCap.capture());
        assertThat(addrCap.getValue().getUserId()).isEqualTo(1001L);
        assertThat(addrCap.getValue().getKeyId()).isEqualTo("k1");
        assertThat(addrCap.getValue().getStatus()).isEqualTo(1);
    }

    @Test
    void allocate_retries_on_uk_collision_and_advances_index() {
        when(keyMaterialService.loadSeed("k1")).thenReturn(new byte[32]);
        // 前两次撞 uk，第三次成功
        when(hdPathMapper.insert(any(HdPathEntity.class)))
                .thenThrow(new DuplicateKeyException("uk_chain_path"))
                .thenThrow(new DuplicateKeyException("uk_chain_path"))
                .thenReturn(1);

        KeyRef ref = service.allocate(1002L, Chain.ETH, "k1");

        // index=2 = m/44'/60'/0'/0/2
        assertThat(ref.getHdPath()).isEqualTo("m/44'/60'/0'/0/2");
        verify(hdPathMapper, atLeast(3)).insert(any(HdPathEntity.class));
        // 派生只在最终成功路径调用一次（不浪费 KMS / wipe 开销）
        verify(keyMaterialService, atLeastOnce()).loadSeed("k1");
        verify(walletAddressMapper).insert(any(WalletAddressEntity.class));
    }

    @Test
    void allocate_unknown_chain_throws_route_error() {
        // 只注入了 ETH derivator，请求 BTC 应抛
        assertThatThrownBy(() -> service.allocate(1003L, Chain.BTC, "k1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no AddressDerivator for chain");
    }

    @Test
    void initRoutingTable_rejects_duplicate_derivator() {
        AddressPoolService dupSvc = new AddressPoolService(
                hdPathMapper, walletAddressMapper, keyMaterialService,
                List.of(new StubEthDerivator(), new StubEthDerivator()));

        assertThatThrownBy(dupSvc::initRoutingTable)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate AddressDerivator");
    }

    /** 测试用 stub：返回固定地址，不真正派生密钥。 */
    static class StubEthDerivator implements AddressDerivator {
        @Override public Chain chain() { return Chain.ETH; }
        @Override public DerivedAddress derive(byte[] hdSeed, String hdPath) {
            return DerivedAddress.builder()
                    .chain(Chain.ETH)
                    .address("0xstub-" + hdPath)
                    .hdPath(hdPath)
                    .publicKeyHex("aabb")
                    .build();
        }
    }
}
