package com.exchange.common.mq.consumed;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import java.time.LocalDateTime;

@Mapper
public interface ConsumedRecordMapper {

    @Select("""
        SELECT COUNT(*) FROM consumed_record
        WHERE event_id = #{eventId} AND handler_name = #{handler}
        """)
    int countByKey(@Param("eventId") String eventId,
                   @Param("handler") String handler);

    @Insert("""
        INSERT IGNORE INTO consumed_record(event_id, handler_name, consumed_at)
        VALUES(#{eventId}, #{handler}, #{consumedAt})
        """)
    int insertIgnore(@Param("eventId") String eventId,
                     @Param("handler") String handler,
                     @Param("consumedAt") LocalDateTime consumedAt);
}
