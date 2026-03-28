package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.UserEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    public KafkaConsumerService(StringRedisTemplate redisTemplate,
                                ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = {
                    "${app.kafka.topics.user-created:user-events.created}",
                    "${app.kafka.topics.user-deleted:user-events.deleted}"
            },
            groupId = "user-group"
    )
    public void consume(String message) {

        try {

            UserEvent event = objectMapper.readValue(message, UserEvent.class);

            log.info("Kafka user event received: {}", event);

            if ("USER_DELETED".equals(event.getEventType())) {

                String key = "user:" + event.getUserId();

                redisTemplate.delete(key);

                log.info("Invalidated cache for user {}", event.getUserId());
            }

        } catch (Exception e) {
            log.error("Failed to consume Kafka user event payload {}", message, e);
        }
    }
}
