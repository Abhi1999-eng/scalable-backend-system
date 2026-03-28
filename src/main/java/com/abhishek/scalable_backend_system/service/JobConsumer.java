package com.abhishek.scalable_backend_system.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class JobConsumer {

    private static final Logger log = LoggerFactory.getLogger(JobConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final JobRunService jobRunService;

    public JobConsumer(StringRedisTemplate redisTemplate, JobRunService jobRunService) {
        this.redisTemplate = redisTemplate;
        this.jobRunService = jobRunService;
    }

    @KafkaListener(topics = "${app.kafka.topics.job-steps:job-runs.steps}", groupId = "job-group")
    public void consume(String message) throws InterruptedException {

        String[] parts = message.split(":");
        long jobId = Long.parseLong(parts[0]);

        try {
            Thread.sleep(50);
            String key = "job:" + jobId + ":progress";
            String current = redisTemplate.opsForValue().get(key);
            int progress = current == null ? 0 : Integer.parseInt(current);
            progress++;
            redisTemplate.opsForValue().set(key, String.valueOf(progress));
            jobRunService.incrementProgress(jobId);
            log.info("Job {} progress updated to {}", jobId, progress);
        } catch (Exception exception) {
            jobRunService.markFailed(jobId, exception.getMessage());
            throw exception;
        }
    }
}
