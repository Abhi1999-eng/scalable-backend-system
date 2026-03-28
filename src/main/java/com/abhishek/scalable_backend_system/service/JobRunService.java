package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.JobRun;
import com.abhishek.scalable_backend_system.repository.JobRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Service
public class JobRunService {

    private static final Logger log = LoggerFactory.getLogger(JobRunService.class);

    private final JobRunRepository jobRunRepository;
    private final long slowJobThresholdMs;

    public JobRunService(
            JobRunRepository jobRunRepository,
            @Value("${app.monitoring.job-slow-threshold-ms:10000}") long slowJobThresholdMs) {
        this.jobRunRepository = jobRunRepository;
        this.slowJobThresholdMs = slowJobThresholdMs;
    }

    public JobRun createQueued(long jobId, int totalSteps, String topicName) {
        JobRun jobRun = new JobRun();
        jobRun.setJobId(jobId);
        jobRun.setTopicName(topicName);
        jobRun.setStatus("QUEUED");
        jobRun.setTotalSteps(totalSteps);
        jobRun.setProcessedSteps(0);
        jobRun.setProgressPercentage(0);
        jobRun.setRetryCount(0);
        jobRun.setCreatedAt(Instant.now());
        return jobRunRepository.save(jobRun);
    }

    public Optional<JobRun> get(long jobId) {
        return jobRunRepository.findByJobId(jobId);
    }

    public JobRun markProcessing(long jobId) {
        JobRun jobRun = require(jobId);
        if (!"PROCESSING".equals(jobRun.getStatus())) {
            jobRun.setStatus("PROCESSING");
            jobRun.setStartedAt(Instant.now());
        }
        return jobRunRepository.save(jobRun);
    }

    public JobRun incrementProgress(long jobId) {
        JobRun jobRun = markProcessing(jobId);
        int next = Math.min(jobRun.getProcessedSteps() + 1, jobRun.getTotalSteps());
        jobRun.setProcessedSteps(next);
        jobRun.setProgressPercentage(jobRun.getTotalSteps() == 0 ? 0 : (int) Math.round((next * 100.0) / jobRun.getTotalSteps()));
        if (next >= jobRun.getTotalSteps()) {
            jobRun.setStatus("COMPLETED");
            jobRun.setFinishedAt(Instant.now());
            if (jobRun.getStartedAt() != null) {
                jobRun.setProcessingTimeMs(Duration.between(jobRun.getStartedAt(), jobRun.getFinishedAt()).toMillis());
                if (jobRun.getProcessingTimeMs() != null && jobRun.getProcessingTimeMs() > slowJobThresholdMs) {
                    log.warn(
                            "Job {} completed slowly in {} ms on topic {}",
                            jobId,
                            jobRun.getProcessingTimeMs(),
                            jobRun.getTopicName()
                    );
                }
            }
        }
        return jobRunRepository.save(jobRun);
    }

    public JobRun markFailed(long jobId, String errorMessage) {
        JobRun jobRun = require(jobId);
        jobRun.setStatus("FAILED");
        jobRun.setErrorMessage(errorMessage);
        jobRun.setFinishedAt(Instant.now());
        if (jobRun.getStartedAt() != null) {
            jobRun.setProcessingTimeMs(Duration.between(jobRun.getStartedAt(), jobRun.getFinishedAt()).toMillis());
        }
        log.warn("Job {} failed after {} steps: {}", jobId, jobRun.getProcessedSteps(), errorMessage);
        return jobRunRepository.save(jobRun);
    }

    public JobRun retry(long jobId) {
        JobRun jobRun = require(jobId);
        jobRun.setStatus("QUEUED");
        jobRun.setProcessedSteps(0);
        jobRun.setProgressPercentage(0);
        jobRun.setErrorMessage(null);
        jobRun.setStartedAt(null);
        jobRun.setFinishedAt(null);
        jobRun.setProcessingTimeMs(null);
        jobRun.setRetryCount(jobRun.getRetryCount() + 1);
        return jobRunRepository.save(jobRun);
    }

    private JobRun require(long jobId) {
        return jobRunRepository.findByJobId(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found."));
    }
}
