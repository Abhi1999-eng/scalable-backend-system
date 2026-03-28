package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.DatasetJob;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface DatasetJobRepository extends JpaRepository<DatasetJob, Long> {

    Optional<DatasetJob> findByDatasetId(String datasetId);

    Optional<DatasetJob> findByJobId(String jobId);

    List<DatasetJob> findByBatchId(String batchId);
}
