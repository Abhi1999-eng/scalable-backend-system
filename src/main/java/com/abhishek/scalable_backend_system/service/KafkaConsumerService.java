package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.UserEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    public KafkaConsumerService(StringRedisTemplate redisTemplate,
                                ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "user-events", groupId = "user-group")
    public void consume(String message) {

        try {

            UserEvent event = objectMapper.readValue(message, UserEvent.class);

            System.out.println("Kafka Event Received → " + event);

            if ("USER_DELETED".equals(event.getEventType())) {

                String key = "user:" + event.getUserId();

                redisTemplate.delete(key);

                System.out.println("Cache invalidated for user " + event.getUserId());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}