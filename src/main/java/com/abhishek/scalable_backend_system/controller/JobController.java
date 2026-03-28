package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.model.JobRun;
import com.abhishek.scalable_backend_system.service.JobRunService;
import com.abhishek.scalable_backend_system.service.KafkaProducerService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/jobs")
public class JobController {

    private final KafkaProducerService producer;
    private final StringRedisTemplate redisTemplate;
    private final JobRunService jobRunService;

    public JobController(KafkaProducerService producer,
                         StringRedisTemplate redisTemplate,
                         JobRunService jobRunService) {
        this.producer = producer;
        this.redisTemplate = redisTemplate;
        this.jobRunService = jobRunService;
    }

    @PostMapping("/start")
    public ResponseEntity<JobRun> startJob(
            @RequestParam(defaultValue = "100") @Min(1) @Max(10000) int totalSteps) {

        long jobId = System.currentTimeMillis() % 100000;
        int total = totalSteps;

        redisTemplate.opsForValue().set("job:" + jobId + ":total", String.valueOf(total));
        redisTemplate.opsForValue().set("job:" + jobId + ":progress", "0");
        JobRun jobRun = jobRunService.createQueued(jobId, total, "job-topic");

        for (int i = 1; i <= total; i++) {
            producer.sendJobEvent((int) jobId, i);
        }

        return ResponseEntity.accepted().body(jobRun);
    }

    @PostMapping("/{jobId}/retry")
    public ResponseEntity<JobRun> retryJob(@PathVariable @Min(1) long jobId) {
        JobRun retried = jobRunService.retry(jobId);
        redisTemplate.opsForValue().set("job:" + jobId + ":total", String.valueOf(retried.getTotalSteps()));
        redisTemplate.opsForValue().set("job:" + jobId + ":progress", "0");

        for (int i = 1; i <= retried.getTotalSteps(); i++) {
            producer.sendJobEvent((int) jobId, i);
        }

        return ResponseEntity.accepted().body(retried);
    }
}
