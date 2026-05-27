package com.exchange.wallet.signer;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface KeyMaterialMapper extends BaseMapper<KeyMaterialEntity> {

    @Select("SELECT * FROM key_material WHERE key_id = #{keyId} LIMIT 1")
    KeyMaterialEntity findByKeyId(@Param("keyId") String keyId);
}
