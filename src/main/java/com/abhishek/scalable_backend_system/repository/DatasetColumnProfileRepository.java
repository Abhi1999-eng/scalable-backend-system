package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.DatasetColumnProfile;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DatasetColumnProfileRepository extends JpaRepository<DatasetColumnProfile, Long> {

    List<DatasetColumnProfile> findByDatasetIdOrderByColumnOrderIndexAsc(String datasetId);

    void deleteByDatasetId(String datasetId);
}
