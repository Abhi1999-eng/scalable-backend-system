package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.SavedChart;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface SavedChartRepository extends JpaRepository<SavedChart, Long> {

    List<SavedChart> findByDatasetIdAndOwnerUserIdOrderByUpdatedAtDesc(String datasetId, Long ownerUserId);

    Optional<SavedChart> findByChartIdAndOwnerUserId(String chartId, Long ownerUserId);
}
