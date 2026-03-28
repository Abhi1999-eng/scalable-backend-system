package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.Dataset;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface DatasetRepository extends JpaRepository<Dataset, Long> {

    Optional<Dataset> findByDatasetId(String datasetId);

    Optional<Dataset> findByDatasetIdAndOwnerUserId(String datasetId, Long ownerUserId);

    List<Dataset> findByBatchIdOrderByCreatedAtAsc(String batchId);

    List<Dataset> findByBatchIdAndOwnerUserIdOrderByCreatedAtAsc(String batchId, Long ownerUserId);

    Page<Dataset> findByOwnerUserIdOrderByCreatedAtDesc(Long ownerUserId, Pageable pageable);
}
