package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.Dataset;
import com.abhishek.scalable_backend_system.model.DatasetJob;
import com.abhishek.scalable_backend_system.repository.DatasetJobRepository;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class DatasetJobService {

    private final DatasetJobRepository datasetJobRepository;

    public DatasetJobService(DatasetJobRepository datasetJobRepository) {
        this.datasetJobRepository = datasetJobRepository;
    }

    public DatasetJob createQueuedJob(Dataset dataset) {
        DatasetJob job = datasetJobRepository.findByDatasetId(dataset.getDatasetId())
                .orElseGet(DatasetJob::new);
        if (job.getJobId() == null) {
            job.setJobId(UUID.randomUUID().toString());
            job.setCreatedAt(Instant.now());
        }
        job.setDatasetId(dataset.getDatasetId());
        job.setBatchId(dataset.getBatchId());
        job.setStatus("QUEUED");
        job.setProgressPercentage(0);
        job.setProcessedRows(0);
        job.setTotalRows(0);
        job.setErrorMessage(null);
        job.setStartedAt(null);
        job.setFinishedAt(null);
        job.setProcessingTimeMs(null);
        return datasetJobRepository.save(job);
    }

    public Optional<DatasetJob> getJob(String datasetId) {
        return datasetJobRepository.findByDatasetId(datasetId);
    }

    public List<DatasetJob> getBatchJobs(String batchId) {
        return datasetJobRepository.findByBatchId(batchId);
    }

    public DatasetJob markProcessing(String datasetId) {
        DatasetJob job = requireJob(datasetId);
        job.setStatus("PROCESSING");
        job.setStartedAt(Instant.now());
        job.setFinishedAt(null);
        job.setErrorMessage(null);
        return datasetJobRepository.save(job);
    }

    public DatasetJob updateProgress(String datasetId, long processedRows) {
        DatasetJob job = requireJob(datasetId);
        job.setProcessedRows(processedRows);
        if (job.getTotalRows() > 0) {
            job.setProgressPercentage((int) Math.min(100, Math.round((processedRows * 100.0) / job.getTotalRows())));
        }
        return datasetJobRepository.save(job);
    }

    public DatasetJob setTotalRows(String datasetId, long totalRows) {
        DatasetJob job = requireJob(datasetId);
        job.setTotalRows(totalRows);
        job.setProgressPercentage(totalRows == 0 ? 0 : job.getProgressPercentage());
        return datasetJobRepository.save(job);
    }

    public DatasetJob markCompleted(String datasetId, long totalRows) {
        DatasetJob job = requireJob(datasetId);
        job.setStatus("COMPLETED");
        job.setTotalRows(totalRows);
        job.setProcessedRows(totalRows);
        job.setProgressPercentage(100);
        job.setFinishedAt(Instant.now());
        if (job.getStartedAt() != null) {
            job.setProcessingTimeMs(Duration.between(job.getStartedAt(), job.getFinishedAt()).toMillis());
        }
        return datasetJobRepository.save(job);
    }

    public DatasetJob markFailed(String datasetId, String errorMessage) {
        DatasetJob job = requireJob(datasetId);
        job.setStatus("FAILED");
        job.setFinishedAt(Instant.now());
        job.setErrorMessage(errorMessage);
        if (job.getStartedAt() != null) {
            job.setProcessingTimeMs(Duration.between(job.getStartedAt(), job.getFinishedAt()).toMillis());
        }
        return datasetJobRepository.save(job);
    }

    public DatasetJob retry(String datasetId) {
        DatasetJob job = requireJob(datasetId);
        job.setStatus("QUEUED");
        job.setProgressPercentage(0);
        job.setProcessedRows(0);
        job.setTotalRows(0);
        job.setErrorMessage(null);
        job.setStartedAt(null);
        job.setFinishedAt(null);
        job.setProcessingTimeMs(null);
        job.setRetryCount(job.getRetryCount() + 1);
        return datasetJobRepository.save(job);
    }

    private DatasetJob requireJob(String datasetId) {
        return datasetJobRepository.findByDatasetId(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset job not found."));
    }
}
