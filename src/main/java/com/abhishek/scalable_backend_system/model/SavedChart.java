package com.abhishek.scalable_backend_system.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(
        name = "saved_charts",
        indexes = {
                @Index(name = "idx_saved_charts_chart_id", columnList = "chart_id", unique = true),
                @Index(name = "idx_saved_charts_dataset_owner", columnList = "dataset_id, owner_user_id"),
                @Index(name = "idx_saved_charts_owner_user_id", columnList = "owner_user_id")
        }
)
public class SavedChart {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "chart_id", nullable = false, unique = true, length = 64)
    private String chartId;

    @Column(name = "owner_user_id", nullable = false)
    private Long ownerUserId;

    @Column(name = "dataset_id", nullable = false, length = 64)
    private String datasetId;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "chart_type", nullable = false, length = 32)
    private String chartType;

    @Column(name = "x_column", nullable = false, length = 255)
    private String xColumn;

    @Column(name = "y_column", length = 255)
    private String yColumn;

    @Column(name = "aggregation", nullable = false, length = 32)
    private String aggregation;

    @Column(name = "limit_value", nullable = false)
    private int limitValue;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getChartId() {
        return chartId;
    }

    public void setChartId(String chartId) {
        this.chartId = chartId;
    }

    public Long getOwnerUserId() {
        return ownerUserId;
    }

    public void setOwnerUserId(Long ownerUserId) {
        this.ownerUserId = ownerUserId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getChartType() {
        return chartType;
    }

    public void setChartType(String chartType) {
        this.chartType = chartType;
    }

    public String getXColumn() {
        return xColumn;
    }

    public void setXColumn(String xColumn) {
        this.xColumn = xColumn;
    }

    public String getYColumn() {
        return yColumn;
    }

    public void setYColumn(String yColumn) {
        this.yColumn = yColumn;
    }

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public int getLimitValue() {
        return limitValue;
    }

    public void setLimitValue(int limitValue) {
        this.limitValue = limitValue;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
