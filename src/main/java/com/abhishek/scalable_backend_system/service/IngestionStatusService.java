package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.FileUploadStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class IngestionStatusService {

    private final Map<String, FileUploadStatus> uploads = new ConcurrentHashMap<>();

    public FileUploadStatus create(String uploadId, int totalFiles, String message) {
        FileUploadStatus status = new FileUploadStatus();
        status.setUploadId(uploadId);
        status.setState("QUEUED");
        status.setTotalFiles(totalFiles);
        status.setDiscoveredFiles(totalFiles);
        status.setProgressPercentage(0);
        status.setCurrentStage("QUEUED");
        status.setMessage(message);
        status.setQueuedAt(Instant.now());
        uploads.put(uploadId, status);
        return new FileUploadStatus(status);
    }

    public synchronized void markStarted(String uploadId) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }

        status.setState("PROCESSING");
        status.setStartedAt(Instant.now());
        status.setCurrentStage("PROCESSING");
    }

    public synchronized void setTotalFiles(String uploadId, int totalFiles) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }

        status.setTotalFiles(totalFiles);
        status.setDiscoveredFiles(totalFiles);
        recomputeMetrics(status);
    }

    public synchronized void setCurrentStage(String uploadId, String currentStage) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }
        status.setCurrentStage(currentStage);
    }

    public synchronized void incrementProcessed(String uploadId, long storedBytes) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }

        status.setProcessedFiles(status.getProcessedFiles() + 1);
        status.setStoredBytes(status.getStoredBytes() + storedBytes);
        recomputeMetrics(status);
    }

    public synchronized void incrementFailed(String uploadId) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }

        status.setFailedFiles(status.getFailedFiles() + 1);
        recomputeMetrics(status);
    }

    public synchronized void markCompleted(String uploadId, String message) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }

        status.setState("COMPLETED");
        status.setCurrentStage("COMPLETED");
        status.setMessage(message);
        status.setFinishedAt(Instant.now());
        recomputeMetrics(status);
    }

    public synchronized void markFailed(String uploadId, String message) {
        FileUploadStatus status = uploads.get(uploadId);
        if (status == null) {
            return;
        }

        status.setState("FAILED");
        status.setCurrentStage("FAILED");
        status.setMessage(message);
        status.setFinishedAt(Instant.now());
        recomputeMetrics(status);
    }

    public FileUploadStatus get(String uploadId) {
        FileUploadStatus status = uploads.get(uploadId);
        return status == null ? null : new FileUploadStatus(status);
    }

    private void recomputeMetrics(FileUploadStatus status) {
        int completed = status.getProcessedFiles() + status.getFailedFiles();
        if (status.getTotalFiles() > 0) {
            status.setProgressPercentage((int) Math.min(100, Math.round((completed * 100.0) / status.getTotalFiles())));
        }
        if (status.getStartedAt() != null) {
            Instant end = status.getFinishedAt() != null ? status.getFinishedAt() : Instant.now();
            long processingTimeMs = Math.max(0, end.toEpochMilli() - status.getStartedAt().toEpochMilli());
            status.setProcessingTimeMs(processingTimeMs);
            if (processingTimeMs > 0) {
                status.setThroughputBytesPerSecond((status.getStoredBytes() * 1000) / processingTimeMs);
            }
        }
    }
}
