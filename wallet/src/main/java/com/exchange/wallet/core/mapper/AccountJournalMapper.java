package com.exchange.wallet.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.exchange.wallet.core.entity.AccountJournalEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

@Mapper
public interface AccountJournalMapper extends BaseMapper<AccountJournalEntity> {

    /**
     * 凑零不变量校验：单一币种下，所有 journal 的 (direction × amount) 必须求和为 0。
     */
    @Select("""
            SELECT COALESCE(SUM(direction * amount), 0)
              FROM account_journal
             WHERE coin_id = #{coinId}
            """)
    BigDecimal sumDirectionalAmount(@Param("coinId") long coinId);

    /**
     * 单账户的全量流水净额（用于 reconcile 回放校验 = account.available + frozen）。
     */
    @Select("""
            SELECT COALESCE(SUM(direction * amount), 0)
              FROM account_journal
             WHERE account_id = #{accountId}
            """)
    BigDecimal sumByAccount(@Param("accountId") long accountId);
}
