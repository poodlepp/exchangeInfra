package com.exchange.common.mq.outbox;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import java.time.LocalDateTime;
import java.util.List;

@Mapper
public interface OutboxMapper extends BaseMapper<OutboxEntity> {

    @Select("""
        SELECT * FROM outbox
        WHERE status = #{status}
          AND (next_retry_at IS NULL OR next_retry_at <= #{now})
        ORDER BY id
        LIMIT #{limit}
        """)
    List<OutboxEntity> pickPending(@Param("status") int status,
                                   @Param("now") LocalDateTime now,
                                   @Param("limit") int limit);

    @Update("UPDATE outbox SET status = #{status} WHERE id = #{id}")
    int markStatus(@Param("id") long id, @Param("status") int status);

    @Update("""
        UPDATE outbox
           SET retry_count = retry_count + 1,
               next_retry_at = #{nextRetryAt}
         WHERE id = #{id}
        """)
    int markRetry(@Param("id") long id, @Param("nextRetryAt") LocalDateTime nextRetryAt);
}
