package com.abhishek.scalable_backend_system.model;

import java.util.List;

public class DatasetBatchProgressResponse {

    private String batchId;
    private int datasetCount;
    private int queuedCount;
    private int processingCount;
    private int completedCount;
    private int failedCount;
    private int progressPercentage;
    private long processedRows;
    private long totalRows;
    private List<DatasetDashboardItem> items;

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public int getDatasetCount() {
        return datasetCount;
    }

    public void setDatasetCount(int datasetCount) {
        this.datasetCount = datasetCount;
    }

    public int getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(int queuedCount) {
        this.queuedCount = queuedCount;
    }

    public int getProcessingCount() {
        return processingCount;
    }

    public void setProcessingCount(int processingCount) {
        this.processingCount = processingCount;
    }

    public int getCompletedCount() {
        return completedCount;
    }

    public void setCompletedCount(int completedCount) {
        this.completedCount = completedCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(int failedCount) {
        this.failedCount = failedCount;
    }

    public int getProgressPercentage() {
        return progressPercentage;
    }

    public void setProgressPercentage(int progressPercentage) {
        this.progressPercentage = progressPercentage;
    }

    public long getProcessedRows() {
        return processedRows;
    }

    public void setProcessedRows(long processedRows) {
        this.processedRows = processedRows;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    public List<DatasetDashboardItem> getItems() {
        return items;
    }

    public void setItems(List<DatasetDashboardItem> items) {
        this.items = items;
    }
}
