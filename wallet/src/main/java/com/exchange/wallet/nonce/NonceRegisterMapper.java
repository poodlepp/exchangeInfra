package com.exchange.wallet.nonce;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;

@Mapper
public interface NonceRegisterMapper extends BaseMapper<NonceRegisterEntity> {

    @Select("""
            SELECT chain, address, next_nonce, on_chain_nonce, reconciled_at, version
              FROM nonce_register
             WHERE chain = #{chain} AND address = #{address}
            """)
    NonceRegisterEntity find(@Param("chain") String chain, @Param("address") String address);

    @Update("""
            INSERT IGNORE INTO nonce_register
                  (chain, address, next_nonce, on_chain_nonce, reconciled_at, version)
            VALUES (#{chain}, #{address}, #{nextNonce}, #{onChainNonce}, #{reconciledAt}, 0)
            """)
    int insertIfAbsent(@Param("chain") String chain,
                       @Param("address") String address,
                       @Param("nextNonce") long nextNonce,
                       @Param("onChainNonce") long onChainNonce,
                       @Param("reconciledAt") LocalDateTime reconciledAt);

    @Update("""
            UPDATE nonce_register
               SET next_nonce = next_nonce + 1, version = version + 1
             WHERE chain = #{chain} AND address = #{address} AND version = #{version}
            """)
    int casIncrement(@Param("chain") String chain,
                     @Param("address") String address,
                     @Param("version") int version);

    @Update("""
            UPDATE nonce_register
               SET next_nonce = #{nextNonce},
                   on_chain_nonce = #{onChainNonce},
                   reconciled_at = #{reconciledAt},
                   version = version + 1
             WHERE chain = #{chain} AND address = #{address} AND version = #{version}
            """)
    int reconcile(@Param("chain") String chain,
                  @Param("address") String address,
                  @Param("nextNonce") long nextNonce,
                  @Param("onChainNonce") long onChainNonce,
                  @Param("reconciledAt") LocalDateTime reconciledAt,
                  @Param("version") int version);
}
