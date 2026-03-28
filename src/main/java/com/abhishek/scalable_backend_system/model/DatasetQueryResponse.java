package com.abhishek.scalable_backend_system.model;

import java.util.List;
import java.util.Map;

public class DatasetQueryResponse {

    private String datasetId;
    private String aggregation;
    private String groupByColumn;
    private String aggregateColumn;
    private long totalRows;
    private int limit;
    private int offset;
    private List<Map<String, Object>> rows;
    private List<Map<String, Object>> aggregates;

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public String getGroupByColumn() {
        return groupByColumn;
    }

    public void setGroupByColumn(String groupByColumn) {
        this.groupByColumn = groupByColumn;
    }

    public String getAggregateColumn() {
        return aggregateColumn;
    }

    public void setAggregateColumn(String aggregateColumn) {
        this.aggregateColumn = aggregateColumn;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    public List<Map<String, Object>> getAggregates() {
        return aggregates;
    }

    public void setAggregates(List<Map<String, Object>> aggregates) {
        this.aggregates = aggregates;
    }
}
