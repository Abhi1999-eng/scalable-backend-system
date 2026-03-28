package com.abhishek.scalable_backend_system.model;

import java.time.Instant;

public class FileUploadStatus {

    private String uploadId;
    private String state;
    private int totalFiles;
    private int discoveredFiles;
    private int processedFiles;
    private int failedFiles;
    private int progressPercentage;
    private long storedBytes;
    private long processingTimeMs;
    private long throughputBytesPerSecond;
    private String currentStage;
    private String message;
    private Instant queuedAt;
    private Instant startedAt;
    private Instant finishedAt;

    public FileUploadStatus() {
    }

    public FileUploadStatus(FileUploadStatus source) {
        this.uploadId = source.uploadId;
        this.state = source.state;
        this.totalFiles = source.totalFiles;
        this.discoveredFiles = source.discoveredFiles;
        this.processedFiles = source.processedFiles;
        this.failedFiles = source.failedFiles;
        this.progressPercentage = source.progressPercentage;
        this.storedBytes = source.storedBytes;
        this.processingTimeMs = source.processingTimeMs;
        this.throughputBytesPerSecond = source.throughputBytesPerSecond;
        this.currentStage = source.currentStage;
        this.message = source.message;
        this.queuedAt = source.queuedAt;
        this.startedAt = source.startedAt;
        this.finishedAt = source.finishedAt;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(int totalFiles) {
        this.totalFiles = totalFiles;
    }

    public int getProcessedFiles() {
        return processedFiles;
    }

    public void setProcessedFiles(int processedFiles) {
        this.processedFiles = processedFiles;
    }

    public int getDiscoveredFiles() {
        return discoveredFiles;
    }

    public void setDiscoveredFiles(int discoveredFiles) {
        this.discoveredFiles = discoveredFiles;
    }

    public int getFailedFiles() {
        return failedFiles;
    }

    public void setFailedFiles(int failedFiles) {
        this.failedFiles = failedFiles;
    }

    public int getProgressPercentage() {
        return progressPercentage;
    }

    public void setProgressPercentage(int progressPercentage) {
        this.progressPercentage = progressPercentage;
    }

    public long getStoredBytes() {
        return storedBytes;
    }

    public void setStoredBytes(long storedBytes) {
        this.storedBytes = storedBytes;
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }

    public long getThroughputBytesPerSecond() {
        return throughputBytesPerSecond;
    }

    public void setThroughputBytesPerSecond(long throughputBytesPerSecond) {
        this.throughputBytesPerSecond = throughputBytesPerSecond;
    }

    public String getCurrentStage() {
        return currentStage;
    }

    public void setCurrentStage(String currentStage) {
        this.currentStage = currentStage;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Instant getQueuedAt() {
        return queuedAt;
    }

    public void setQueuedAt(Instant queuedAt) {
        this.queuedAt = queuedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }
}
