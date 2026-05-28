package com.exchange.wallet.core.ledger;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.core.entity.AccountEntity;
import com.exchange.wallet.core.mapper.AccountJournalMapper;
import com.exchange.wallet.core.mapper.AccountMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LedgerServiceImpl 集成测试 — 双账法核心契约。
 *
 * 三大不变量：
 * 1. 凑零不变量：任何币种 SUM(direction × amount) GROUP BY coin_id 必须 == 0
 * 2. 同 traceId 重入：第二次 credit 抛 DuplicateKeyException（uk 闸生效）
 * 3. freeze → settle 余额轨迹正确（user.frozen 减、HOT_WALLET.available 增）
 */
@SpringBootTest(classes = LedgerTestApplication.class)
@ActiveProfiles("ledgerit")
@Testcontainers
class LedgerServiceImplIT {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withUsername("test").withPassword("test").withDatabaseName("ledger_test")
            .waitingFor(Wait.forLogMessage(".*ready for connections.*\\n", 2)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("mysql.url", mysql::getJdbcUrl);
    }

    @Autowired LedgerServiceImpl ledger;
    @Autowired AccountMapper accountMapper;
    @Autowired AccountJournalMapper journalMapper;

    @Test
    void credit_writes_double_journal_and_zero_sum_invariant_holds() {
        long coinId = nextId();
        long userId = nextId();

        ledger.credit(LedgerCommand.builder()
                .traceId(UUID.randomUUID().toString())
                .fromUserId(SystemAccountConstants.INFLOW)
                .toUserId(userId)
                .coinId(coinId)
                .amount(new BigDecimal("1.5"))
                .bizType(BizType.DEPOSIT)
                .bizId(nextId())
                .build());

        AccountEntity user = accountMapper.find(userId, coinId);
        AccountEntity inflow = accountMapper.find(SystemAccountConstants.INFLOW, coinId);
        assertThat(user.getAvailable()).isEqualByComparingTo("1.5");
        assertThat(inflow.getAvailable()).isEqualByComparingTo("-1.5");

        // 凑零不变量
        assertThat(journalMapper.sumDirectionalAmount(coinId)).isEqualByComparingTo("0");
        // 单账户回放：user 账户的全量流水净额 == available + frozen
        assertThat(journalMapper.sumByAccount(user.getId()))
                .isEqualByComparingTo(user.getAvailable().add(user.getFrozen()));
    }

    @Test
    void credit_reentry_with_same_traceId_throws_duplicate_key() {
        long coinId = nextId();
        long userId = nextId();
        String traceId = UUID.randomUUID().toString();
        LedgerCommand cmd = LedgerCommand.builder()
                .traceId(traceId)
                .fromUserId(SystemAccountConstants.INFLOW)
                .toUserId(userId)
                .coinId(coinId)
                .amount(new BigDecimal("2.0"))
                .bizType(BizType.DEPOSIT)
                .bizId(nextId())
                .build();

        ledger.credit(cmd);

        // 第二次同 traceId → uk(trace_id, direction, account_id) 抛
        assertThatThrownBy(() -> ledger.credit(cmd))
                .isInstanceOf(DuplicateKeyException.class);

        // 余额仍是单次结果，凑零不变量仍成立
        AccountEntity user = accountMapper.find(userId, coinId);
        assertThat(user.getAvailable()).isEqualByComparingTo("2.0");
        assertThat(journalMapper.sumDirectionalAmount(coinId)).isEqualByComparingTo("0");
    }

    @Test
    void freeze_then_settle_moves_user_frozen_to_hot_wallet_available() {
        long coinId = nextId();
        long userId = nextId();

        // 准备：用户先充值 10
        ledger.credit(LedgerCommand.builder()
                .traceId(UUID.randomUUID().toString())
                .fromUserId(SystemAccountConstants.INFLOW)
                .toUserId(userId)
                .coinId(coinId)
                .amount(new BigDecimal("10"))
                .bizType(BizType.DEPOSIT)
                .bizId(nextId())
                .build());

        long withdrawOrderId = nextId();

        // freeze 3
        ledger.freeze(LedgerCommand.builder()
                .traceId(UUID.randomUUID().toString())
                .fromUserId(userId)
                .toUserId(userId)
                .coinId(coinId)
                .amount(new BigDecimal("3"))
                .bizType(BizType.WITHDRAW_FREEZE)
                .bizId(withdrawOrderId)
                .build());

        AccountEntity userAfterFreeze = accountMapper.find(userId, coinId);
        assertThat(userAfterFreeze.getAvailable()).isEqualByComparingTo("7");
        assertThat(userAfterFreeze.getFrozen()).isEqualByComparingTo("3");

        // settle 3 → HOT_WALLET
        ledger.settle(LedgerCommand.builder()
                .traceId(UUID.randomUUID().toString())
                .fromUserId(userId)
                .toUserId(SystemAccountConstants.HOT_WALLET)
                .coinId(coinId)
                .amount(new BigDecimal("3"))
                .bizType(BizType.WITHDRAW_SETTLE)
                .bizId(withdrawOrderId)
                .build());

        AccountEntity userFinal = accountMapper.find(userId, coinId);
        AccountEntity hot = accountMapper.find(SystemAccountConstants.HOT_WALLET, coinId);
        assertThat(userFinal.getAvailable()).isEqualByComparingTo("7");
        assertThat(userFinal.getFrozen()).isEqualByComparingTo("0");
        assertThat(hot.getAvailable()).isEqualByComparingTo("3");

        // 凑零不变量贯穿三步操作
        assertThat(journalMapper.sumDirectionalAmount(coinId)).isEqualByComparingTo("0");
    }

    @Test
    void freeze_with_insufficient_available_throws_and_no_journal_written() {
        long coinId = nextId();
        long userId = nextId();
        long bizId = nextId();

        // 没有充值就直接 freeze
        assertThatThrownBy(() -> ledger.freeze(LedgerCommand.builder()
                .traceId(UUID.randomUUID().toString())
                .fromUserId(userId)
                .toUserId(userId)
                .coinId(coinId)
                .amount(new BigDecimal("1"))
                .bizType(BizType.WITHDRAW_FREEZE)
                .bizId(bizId)
                .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("insufficient available");

        // 因 @Transactional 自动回滚，凑零不变量仍 0（甚至 journal 也未落库）
        assertThat(journalMapper.sumDirectionalAmount(coinId)).isEqualByComparingTo("0");
    }

    private static long nextId() {
        return SnowflakeIdGenerator.nextDefaultId();
    }
}
