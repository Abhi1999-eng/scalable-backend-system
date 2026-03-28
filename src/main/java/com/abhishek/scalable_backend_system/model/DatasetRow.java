package com.abhishek.scalable_backend_system.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

@Entity
@Table(
        name = "dataset_rows",
        indexes = {
                @Index(name = "idx_dataset_rows_dataset_id", columnList = "dataset_id")
        }
)
public class DatasetRow {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dataset_id", nullable = false, length = 64)
    private String datasetId;

    @Column(name = "row_number", nullable = false)
    private long rowNumber;

    @Column(name = "row_json", nullable = false, columnDefinition = "TEXT")
    private String rowJson;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public void setRowNumber(long rowNumber) {
        this.rowNumber = rowNumber;
    }

    public String getRowJson() {
        return rowJson;
    }

    public void setRowJson(String rowJson) {
        this.rowJson = rowJson;
    }
}
