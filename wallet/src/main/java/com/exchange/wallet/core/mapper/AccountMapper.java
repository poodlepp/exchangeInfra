package com.exchange.wallet.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.exchange.wallet.core.entity.AccountEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;

@Mapper
public interface AccountMapper extends BaseMapper<AccountEntity> {

    @Select("""
            SELECT id, user_id, coin_id, available, frozen, version, created_at, updated_at
              FROM account
             WHERE user_id = #{userId} AND coin_id = #{coinId}
            """)
    AccountEntity find(@Param("userId") long userId, @Param("coinId") long coinId);

    @Update("""
            UPDATE account
               SET available = #{available},
                   frozen    = #{frozen},
                   version   = version + 1
             WHERE id = #{id} AND version = #{version}
            """)
    int casUpdate(@Param("id") long id,
                  @Param("available") BigDecimal available,
                  @Param("frozen") BigDecimal frozen,
                  @Param("version") int version);
}
