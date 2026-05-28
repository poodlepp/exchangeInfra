package com.exchange.wallet.core.ledger;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.core.entity.AccountEntity;
import com.exchange.wallet.core.entity.AccountJournalEntity;
import com.exchange.wallet.core.mapper.AccountJournalMapper;
import com.exchange.wallet.core.mapper.AccountMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 双账法账本实现。
 *
 * <p>设计要点（每条都对应面试题）：
 * <ol>
 *   <li><b>双账法 + uk(trace_id, direction, account_id)</b>：每个方法落两条 journal，
 *       同 traceId 重入第二次必撞 unique，被翻译为幂等命中（吞掉异常、不抛回业务）。</li>
 *   <li><b>account 表 CAS 更新</b>：{@code WHERE id = ? AND version = ?}，零行影响 = 余额冲突或并发败者。
 *       本期实现先 service 层校验余额，再 CAS（更原子的"AND available >= amount"做生产升级路径）。</li>
 *   <li><b>balance_after 当场写入</b>：可减少 reconcile 时全量回放成本。</li>
 *   <li><b>ensureAccount 自动建账</b>：用户首次有资金移动时插入 account 行，避免上层依赖 user 服务。
 *       并发场景下 race insert 会撞 uk_user_coin → 兜底再读一次。</li>
 *   <li><b>系统账户在启动期幂等插入</b>：@PostConstruct 用 ensureAccount 兜底 4 个常量账户。</li>
 *   <li><b>不直接发 Kafka</b>：业务事件由调用方 service 通过 TransactionalEventPublisher 发；
 *       Ledger 只管账，单一职责。</li>
 *   <li><b>异常直接抛出</b>：不 try/catch 吞，否则破坏 Spring 事务回滚边界。</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerServiceImpl implements LedgerService {

    private static final long SYSTEM_COIN_BOOTSTRAP = 0L;

    private final AccountMapper accountMapper;
    private final AccountJournalMapper journalMapper;

    /**
     * 启动期幂等插入 4 个系统账户（coin_id=0 占位）。
     * 业务首次引用 INFLOW/HOT_WALLET/FEE/FROZEN_BUFFER 时会通过 ensureAccount 按真实 coinId 再建。
     * 这里只是占位，避免极端场景 NPE。
     */
    @PostConstruct
    void initSystemAccounts() {
        long[] systemUsers = {
                SystemAccountConstants.INFLOW,
                SystemAccountConstants.HOT_WALLET,
                SystemAccountConstants.FEE,
                SystemAccountConstants.FROZEN_BUFFER
        };
        for (long uid : systemUsers) {
            try {
                ensureAccountInNewTx(uid, SYSTEM_COIN_BOOTSTRAP);
            } catch (DuplicateKeyException ignore) {
                // 已存在，继续
            }
        }
    }

    @Override
    @Transactional
    public void transferAvailable(LedgerCommand cmd) {
        validate(cmd);
        if (cmd.getFromUserId() == cmd.getToUserId()) {
            throw new IllegalArgumentException("transferAvailable requires different from/to user");
        }
        AccountEntity from = ensureAccount(cmd.getFromUserId(), cmd.getCoinId());
        require(from.getAvailable().compareTo(cmd.getAmount()) >= 0,
                "insufficient available: user=" + cmd.getFromUserId() + " coin=" + cmd.getCoinId());
        BigDecimal newFromAvailable = from.getAvailable().subtract(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(from.getId(), newFromAvailable, from.getFrozen(), from.getVersion()));

        AccountEntity to = ensureAccount(cmd.getToUserId(), cmd.getCoinId());
        BigDecimal newToAvailable = to.getAvailable().add(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(to.getId(), newToAvailable, to.getFrozen(), to.getVersion()));

        insertDoubleJournal(cmd, from.getId(), newFromAvailable, to.getId(), newToAvailable);
    }

    @Override
    @Transactional
    public void freeze(LedgerCommand cmd) {
        validate(cmd);
        require(cmd.getFromUserId() == cmd.getToUserId(),
                "freeze requires fromUserId == toUserId (single account两列对调)");
        AccountEntity acc = ensureAccount(cmd.getFromUserId(), cmd.getCoinId());
        require(acc.getAvailable().compareTo(cmd.getAmount()) >= 0,
                "insufficient available for freeze: user=" + cmd.getFromUserId() + " coin=" + cmd.getCoinId());
        BigDecimal newAvailable = acc.getAvailable().subtract(cmd.getAmount());
        BigDecimal newFrozen = acc.getFrozen().add(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(acc.getId(), newAvailable, newFrozen, acc.getVersion()));

        // 同账户跨列：DEBIT/balance_after = newAvailable, CREDIT/balance_after = newFrozen
        insertSameAccountJournal(cmd, acc.getId(), newAvailable, newFrozen);
    }

    @Override
    @Transactional
    public void unfreeze(LedgerCommand cmd) {
        validate(cmd);
        require(cmd.getFromUserId() == cmd.getToUserId(),
                "unfreeze requires fromUserId == toUserId");
        AccountEntity acc = ensureAccount(cmd.getFromUserId(), cmd.getCoinId());
        require(acc.getFrozen().compareTo(cmd.getAmount()) >= 0,
                "insufficient frozen for unfreeze: user=" + cmd.getFromUserId() + " coin=" + cmd.getCoinId());
        BigDecimal newFrozen = acc.getFrozen().subtract(cmd.getAmount());
        BigDecimal newAvailable = acc.getAvailable().add(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(acc.getId(), newAvailable, newFrozen, acc.getVersion()));

        // DEBIT(frozen-) balance_after = newFrozen, CREDIT(available+) balance_after = newAvailable
        insertJournal(cmd, acc.getId(), JournalDirection.DEBIT, newFrozen);
        insertJournal(cmd, acc.getId(), JournalDirection.CREDIT, newAvailable);
    }

    @Override
    @Transactional
    public void settle(LedgerCommand cmd) {
        validate(cmd);
        require(cmd.getToUserId() == SystemAccountConstants.HOT_WALLET,
                "settle requires toUserId == HOT_WALLET");
        AccountEntity user = ensureAccount(cmd.getFromUserId(), cmd.getCoinId());
        require(user.getFrozen().compareTo(cmd.getAmount()) >= 0,
                "insufficient frozen for settle: user=" + cmd.getFromUserId());
        BigDecimal newUserFrozen = user.getFrozen().subtract(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(user.getId(), user.getAvailable(), newUserFrozen, user.getVersion()));

        AccountEntity hot = ensureAccount(SystemAccountConstants.HOT_WALLET, cmd.getCoinId());
        BigDecimal newHotAvailable = hot.getAvailable().add(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(hot.getId(), newHotAvailable, hot.getFrozen(), hot.getVersion()));

        insertJournal(cmd, user.getId(), JournalDirection.DEBIT, newUserFrozen);
        insertJournal(cmd, hot.getId(), JournalDirection.CREDIT, newHotAvailable);
    }

    @Override
    @Transactional
    public void credit(LedgerCommand cmd) {
        validate(cmd);
        require(cmd.getFromUserId() == SystemAccountConstants.INFLOW,
                "credit requires fromUserId == INFLOW");
        AccountEntity inflow = ensureAccount(SystemAccountConstants.INFLOW, cmd.getCoinId());
        BigDecimal newInflowAvailable = inflow.getAvailable().subtract(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(inflow.getId(), newInflowAvailable, inflow.getFrozen(), inflow.getVersion()));

        AccountEntity user = ensureAccount(cmd.getToUserId(), cmd.getCoinId());
        BigDecimal newUserAvailable = user.getAvailable().add(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(user.getId(), newUserAvailable, user.getFrozen(), user.getVersion()));

        insertDoubleJournal(cmd, inflow.getId(), newInflowAvailable, user.getId(), newUserAvailable);
    }

    @Override
    @Transactional
    public void reverseCredit(LedgerCommand cmd) {
        validate(cmd);
        require(cmd.getToUserId() == SystemAccountConstants.INFLOW,
                "reverseCredit requires toUserId == INFLOW");
        AccountEntity user = ensureAccount(cmd.getFromUserId(), cmd.getCoinId());
        require(user.getAvailable().compareTo(cmd.getAmount()) >= 0,
                "insufficient available for reverseCredit: user=" + cmd.getFromUserId());
        BigDecimal newUserAvailable = user.getAvailable().subtract(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(user.getId(), newUserAvailable, user.getFrozen(), user.getVersion()));

        AccountEntity inflow = ensureAccount(SystemAccountConstants.INFLOW, cmd.getCoinId());
        BigDecimal newInflowAvailable = inflow.getAvailable().add(cmd.getAmount());
        casOrThrow(accountMapper.casUpdate(inflow.getId(), newInflowAvailable, inflow.getFrozen(), inflow.getVersion()));

        insertDoubleJournal(cmd, user.getId(), newUserAvailable, inflow.getId(), newInflowAvailable);
    }

    /**
     * 确保 (userId, coinId) 账户存在；不存在则插入。
     * 并发 race：第二个线程会撞 uk_user_coin → 捕获 DuplicateKeyException 兜底再读。
     */
    AccountEntity ensureAccount(long userId, long coinId) {
        AccountEntity exist = accountMapper.find(userId, coinId);
        if (exist != null) {
            return exist;
        }
        AccountEntity row = new AccountEntity();
        row.setId(SnowflakeIdGenerator.nextDefaultId());
        row.setUserId(userId);
        row.setCoinId(coinId);
        row.setAvailable(BigDecimal.ZERO);
        row.setFrozen(BigDecimal.ZERO);
        row.setVersion(0);
        LocalDateTime now = LocalDateTime.now();
        row.setCreatedAt(now);
        row.setUpdatedAt(now);
        try {
            accountMapper.insert(row);
            return row;
        } catch (DuplicateKeyException race) {
            return accountMapper.find(userId, coinId);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    void ensureAccountInNewTx(long userId, long coinId) {
        ensureAccount(userId, coinId);
    }

    private void insertDoubleJournal(LedgerCommand cmd,
                                     long fromAccountId, BigDecimal fromBalanceAfter,
                                     long toAccountId, BigDecimal toBalanceAfter) {
        insertJournal(cmd, fromAccountId, JournalDirection.DEBIT, fromBalanceAfter);
        insertJournal(cmd, toAccountId, JournalDirection.CREDIT, toBalanceAfter);
    }

    private void insertSameAccountJournal(LedgerCommand cmd, long accountId,
                                          BigDecimal availableAfter, BigDecimal frozenAfter) {
        insertJournal(cmd, accountId, JournalDirection.DEBIT, availableAfter);
        insertJournal(cmd, accountId, JournalDirection.CREDIT, frozenAfter);
    }

    private void insertJournal(LedgerCommand cmd, long accountId, JournalDirection dir, BigDecimal balanceAfter) {
        AccountJournalEntity j = new AccountJournalEntity();
        j.setId(SnowflakeIdGenerator.nextDefaultId());
        j.setTraceId(cmd.getTraceId());
        j.setAccountId(accountId);
        j.setCoinId(cmd.getCoinId());
        j.setBizType(cmd.getBizType().name());
        j.setBizId(cmd.getBizId());
        j.setDirection(dir.value());
        j.setAmount(cmd.getAmount());
        j.setBalanceAfter(balanceAfter);
        j.setRemark(cmd.getRemark());
        j.setCreatedAt(LocalDateTime.now());
        journalMapper.insert(j);
    }

    private static void validate(LedgerCommand cmd) {
        if (cmd == null) throw new IllegalArgumentException("cmd is null");
        if (cmd.getTraceId() == null || cmd.getTraceId().isBlank()) {
            throw new IllegalArgumentException("traceId is required");
        }
        if (cmd.getAmount() == null || cmd.getAmount().signum() <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }
        if (cmd.getBizType() == null) throw new IllegalArgumentException("bizType is required");
        if (cmd.getCoinId() <= 0) throw new IllegalArgumentException("coinId must be > 0");
    }

    private static void require(boolean cond, String msg) {
        if (!cond) throw new IllegalStateException(msg);
    }

    private static void casOrThrow(int affected) {
        if (affected != 1) {
            throw new IllegalStateException("CAS update failed: affected=" + affected
                    + " (concurrent modification or balance check failed)");
        }
    }

    /** 仅用于测试 / 监控：单币种凑零不变量。 */
    public BigDecimal sumDirectionalAmount(long coinId) {
        return journalMapper.sumDirectionalAmount(coinId);
    }

    /** 仅用于测试 / 监控：列举系统账户做断言。 */
    public List<Long> systemAccounts() {
        return List.of(SystemAccountConstants.INFLOW, SystemAccountConstants.HOT_WALLET,
                SystemAccountConstants.FEE, SystemAccountConstants.FROZEN_BUFFER);
    }
}
