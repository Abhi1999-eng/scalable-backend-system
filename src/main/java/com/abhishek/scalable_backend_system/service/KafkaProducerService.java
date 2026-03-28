package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.UserEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final boolean kafkaEnabled;

    private final String userCreatedTopic;
    private final String userDeletedTopic;
    private final String jobStepsTopic;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.enabled:true}") boolean kafkaEnabled,
            @Value("${app.kafka.topics.user-created:user-events.created}") String userCreatedTopic,
            @Value("${app.kafka.topics.user-deleted:user-events.deleted}") String userDeletedTopic,
            @Value("${app.kafka.topics.job-steps:job-runs.steps}") String jobStepsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.kafkaEnabled = kafkaEnabled;
        this.userCreatedTopic = userCreatedTopic;
        this.userDeletedTopic = userDeletedTopic;
        this.jobStepsTopic = jobStepsTopic;
    }

    public void sendUserCreatedEvent(Long userId) {

        try {
            if (!kafkaEnabled) {
                log.debug("Kafka is disabled; skipping user created event for user {}", userId);
                return;
            }

            UserEvent event = new UserEvent(
                    "USER_CREATED",
                    userId,
                    System.currentTimeMillis());

            String json = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(userCreatedTopic, String.valueOf(userId), json);

            log.info("Kafka user event sent: {}", json);

        } catch (Exception e) {
            log.error("Failed to send user created event for user {}", userId, e);
        }
    }

    public void sendUserDeletedEvent(Long userId) {

        try {
            if (!kafkaEnabled) {
                log.debug("Kafka is disabled; skipping user deleted event for user {}", userId);
                return;
            }

            UserEvent event = new UserEvent(
                    "USER_DELETED",
                    userId,
                    System.currentTimeMillis());

            String json = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(userDeletedTopic, String.valueOf(userId), json);

            log.info("Kafka user event sent: {}", json);

        } catch (Exception e) {
            log.error("Failed to send user deleted event for user {}", userId, e);
        }
    }

    public void sendJobEvent(int jobId, int step) {
        if (!kafkaEnabled) {
            log.debug("Kafka is disabled; skipping job event {}:{}", jobId, step);
            return;
        }
        String message = jobId + ":" + step;
        kafkaTemplate.send(jobStepsTopic, String.valueOf(jobId), message);
        log.debug("Kafka job event sent: {}", message);
    }
}
