package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.DatasetRow;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DatasetRowRepository extends JpaRepository<DatasetRow, Long> {

    List<DatasetRow> findByDatasetIdOrderByRowNumberAsc(String datasetId, Pageable pageable);

    List<DatasetRow> findByDatasetId(String datasetId);

    void deleteByDatasetId(String datasetId);
}
