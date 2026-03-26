package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.UserEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC = "user-events";

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendUserCreatedEvent(Long userId) {

        try {

            UserEvent event = new UserEvent(
                    "USER_CREATED",
                    userId,
                    System.currentTimeMillis());

            String json = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(TOPIC, json);

            System.out.println("Kafka Event Sent → " + json);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendUserDeletedEvent(Long userId) {

        try {

            UserEvent event = new UserEvent(
                    "USER_DELETED",
                    userId,
                    System.currentTimeMillis());

            String json = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(TOPIC, json);

            System.out.println("Kafka Event Sent → " + json);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendJobEvent(int jobId, int step) {
        String message = jobId + ":" + step;
        kafkaTemplate.send("job-topic", message);
    }
}