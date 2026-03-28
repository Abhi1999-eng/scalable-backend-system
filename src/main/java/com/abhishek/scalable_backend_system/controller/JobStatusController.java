package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.model.JobRun;
import com.abhishek.scalable_backend_system.service.JobRunService;
import jakarta.validation.constraints.Min;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/jobs")
public class JobStatusController {

    private final StringRedisTemplate redisTemplate;
    private final JobRunService jobRunService;

    public JobStatusController(StringRedisTemplate redisTemplate, JobRunService jobRunService) {
        this.redisTemplate = redisTemplate;
        this.jobRunService = jobRunService;
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobRun> getStatus(@PathVariable @Min(1) long jobId) {
        JobRun jobRun = jobRunService.get(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found."));

        String progress = redisTemplate.opsForValue().get("job:" + jobId + ":progress");
        String total = redisTemplate.opsForValue().get("job:" + jobId + ":total");

        jobRun.setProcessedSteps(progress == null ? 0 : Integer.parseInt(progress));
        jobRun.setTotalSteps(total == null ? 0 : Integer.parseInt(total));
        jobRun.setProgressPercentage(jobRun.getTotalSteps() == 0 ? 0
                : (int) Math.round((jobRun.getProcessedSteps() * 100.0) / jobRun.getTotalSteps()));

        return ResponseEntity.ok(jobRun);
    }
}
