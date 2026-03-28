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
        name = "dataset_column_profiles",
        indexes = {
                @Index(name = "idx_dataset_column_profiles_dataset_id", columnList = "dataset_id")
        }
)
public class DatasetColumnProfile {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dataset_id", nullable = false, length = 64)
    private String datasetId;

    @Column(name = "column_name", nullable = false, length = 255)
    private String columnName;

    @Column(name = "column_order_index", nullable = false)
    private int columnOrderIndex;

    @Column(name = "inferred_type", nullable = false, length = 32)
    private String inferredType;

    @Column(name = "non_null_count", nullable = false)
    private long nonNullCount;

    @Column(name = "null_count", nullable = false)
    private long nullCount;

    @Column(name = "distinct_count", nullable = false)
    private long distinctCount;

    @Column(name = "sample_value", length = 1000)
    private String sampleValue;

    @Column(name = "min_value", length = 255)
    private String minValue;

    @Column(name = "max_value", length = 255)
    private String maxValue;

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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getColumnOrderIndex() {
        return columnOrderIndex;
    }

    public void setColumnOrderIndex(int columnOrderIndex) {
        this.columnOrderIndex = columnOrderIndex;
    }

    public String getInferredType() {
        return inferredType;
    }

    public void setInferredType(String inferredType) {
        this.inferredType = inferredType;
    }

    public long getNonNullCount() {
        return nonNullCount;
    }

    public void setNonNullCount(long nonNullCount) {
        this.nonNullCount = nonNullCount;
    }

    public long getNullCount() {
        return nullCount;
    }

    public void setNullCount(long nullCount) {
        this.nullCount = nullCount;
    }

    public long getDistinctCount() {
        return distinctCount;
    }

    public void setDistinctCount(long distinctCount) {
        this.distinctCount = distinctCount;
    }

    public String getSampleValue() {
        return sampleValue;
    }

    public void setSampleValue(String sampleValue) {
        this.sampleValue = sampleValue;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }
}
