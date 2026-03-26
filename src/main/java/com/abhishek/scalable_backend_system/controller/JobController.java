package com.abhishek.scalable_backend_system.controller;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.abhishek.scalable_backend_system.service.KafkaProducerService;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final KafkaProducerService producer;
    private final StringRedisTemplate redisTemplate;

    public JobController(KafkaProducerService producer,
                         StringRedisTemplate redisTemplate) {
        this.producer = producer;
        this.redisTemplate = redisTemplate;
    }

    @PostMapping("/start")
    public String startJob() {

        int jobId = (int) (System.currentTimeMillis() % 100000);
        int total = 100;

        redisTemplate.opsForValue().set("job:" + jobId + ":total", String.valueOf(total));
        redisTemplate.opsForValue().set("job:" + jobId + ":progress", "0");

        for (int i = 1; i <= total; i++) {
            producer.sendJobEvent(jobId, i);
        }

        return "Job started with ID: " + jobId;
    }
}