package com.exchange.wallet.nonce;

import com.exchange.wallet.chain.api.Chain;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DbOptimisticNonceAllocatorTest {

    private NonceRegisterMapper mapper;
    private DbOptimisticNonceAllocator allocator;

    @BeforeEach
    void setUp() {
        mapper = mock(NonceRegisterMapper.class);
        allocator = new DbOptimisticNonceAllocator(mapper);
    }

    @Test
    void allocate_success_first_try() {
        NonceRegisterEntity row = row(7, 3);
        when(mapper.find("ETH", "0xabc")).thenReturn(row);
        when(mapper.casIncrement("ETH", "0xabc", 3)).thenReturn(1);

        long n = allocator.allocate(Chain.ETH, "0xabc");

        assertThat(n).isEqualTo(7L);
        verify(mapper, times(1)).casIncrement(anyString(), anyString(), anyInt());
    }

    @Test
    void allocate_retries_on_cas_conflict() {
        when(mapper.find("ETH", "0xabc"))
                .thenReturn(row(7, 3))
                .thenReturn(row(8, 4))
                .thenReturn(row(9, 5));
        when(mapper.casIncrement("ETH", "0xabc", 3)).thenReturn(0);
        when(mapper.casIncrement("ETH", "0xabc", 4)).thenReturn(0);
        when(mapper.casIncrement("ETH", "0xabc", 5)).thenReturn(1);

        long n = allocator.allocate(Chain.ETH, "0xabc");

        assertThat(n).isEqualTo(9L);
        verify(mapper, times(3)).casIncrement(anyString(), anyString(), anyInt());
    }

    @Test
    void allocate_throws_when_not_initialized() {
        when(mapper.find("ETH", "0xabc")).thenReturn(null);

        assertThatThrownBy(() -> allocator.allocate(Chain.ETH, "0xabc"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not initialized");
    }

    @Test
    void allocate_throws_after_max_retries() {
        when(mapper.find("ETH", "0xabc")).thenReturn(row(7, 3));
        when(mapper.casIncrement(anyString(), anyString(), anyInt())).thenReturn(0);

        assertThatThrownBy(() -> allocator.allocate(Chain.ETH, "0xabc"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CAS failed");
        verify(mapper, times(5)).casIncrement(anyString(), anyString(), anyInt());
    }

    @Test
    void reconcile_inserts_if_row_absent() {
        when(mapper.find("ETH", "0xabc")).thenReturn(null);

        allocator.reconcile(Chain.ETH, "0xabc", 42);

        verify(mapper).insertIfAbsent(eq("ETH"), eq("0xabc"), eq(42L), eq(42L), any(LocalDateTime.class));
        verify(mapper, never()).reconcile(anyString(), anyString(), anyLong(), anyLong(), any(), anyInt());
    }

    @Test
    void reconcile_advances_when_local_lags_chain() {
        when(mapper.find("ETH", "0xabc")).thenReturn(row(5, 2));
        when(mapper.reconcile(eq("ETH"), eq("0xabc"), eq(10L), eq(10L), any(), eq(2))).thenReturn(1);

        allocator.reconcile(Chain.ETH, "0xabc", 10);

        verify(mapper).reconcile(eq("ETH"), eq("0xabc"), eq(10L), eq(10L), any(LocalDateTime.class), eq(2));
    }

    @Test
    void reconcile_skips_when_local_already_ahead() {
        when(mapper.find("ETH", "0xabc")).thenReturn(row(20, 5));

        allocator.reconcile(Chain.ETH, "0xabc", 10);

        verify(mapper, never()).reconcile(anyString(), anyString(), anyLong(), anyLong(), any(), anyInt());
        verify(mapper, never()).insertIfAbsent(anyString(), anyString(), anyLong(), anyLong(), any());
    }

    private static NonceRegisterEntity row(long nextNonce, int version) {
        NonceRegisterEntity e = new NonceRegisterEntity();
        e.setChain("ETH");
        e.setAddress("0xabc");
        e.setNextNonce(nextNonce);
        e.setOnChainNonce(nextNonce);
        e.setReconciledAt(LocalDateTime.now());
        e.setVersion(version);
        return e;
    }
}
