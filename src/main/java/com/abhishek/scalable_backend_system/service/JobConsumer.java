package com.abhishek.scalable_backend_system.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JobConsumer {

    private final StringRedisTemplate redisTemplate;

    public JobConsumer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = "job-topic", groupId = "job-group")
    public void consume(String message) throws InterruptedException {

        String[] parts = message.split(":");
        int jobId = Integer.parseInt(parts[0]);

        // simulate work
        Thread.sleep(50);

        String key = "job:" + jobId + ":progress";

        String current = redisTemplate.opsForValue().get(key);
        int progress = current == null ? 0 : Integer.parseInt(current);

        progress++;

        redisTemplate.opsForValue().set(key, String.valueOf(progress));

        System.out.println("Job " + jobId + " progress: " + progress);
    }
}