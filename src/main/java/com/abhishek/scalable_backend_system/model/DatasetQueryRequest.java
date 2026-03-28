package com.abhishek.scalable_backend_system.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

import java.util.ArrayList;
import java.util.List;

public class DatasetQueryRequest {

    @Valid
    private List<DatasetQueryFilter> filters = new ArrayList<>();

    @Valid
    private List<DatasetQuerySort> sorts = new ArrayList<>();

    private String groupByColumn;

    private String aggregateColumn;

    @Pattern(
            regexp = "count|sum|avg|histogram",
            flags = Pattern.Flag.CASE_INSENSITIVE,
            message = "aggregation must be one of count, sum, avg, histogram"
    )
    private String aggregation = "count";

    @Min(value = 1, message = "limit must be at least 1")
    @Max(value = 500, message = "limit must be at most 500")
    private Integer limit = 25;

    @Min(value = 0, message = "offset must be at least 0")
    private Integer offset = 0;

    @Min(value = 1, message = "histogramBins must be at least 1")
    @Max(value = 50, message = "histogramBins must be at most 50")
    private Integer histogramBins = 10;

    public List<DatasetQueryFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<DatasetQueryFilter> filters) {
        this.filters = filters == null ? new ArrayList<>() : filters;
    }

    public List<DatasetQuerySort> getSorts() {
        return sorts;
    }

    public void setSorts(List<DatasetQuerySort> sorts) {
        this.sorts = sorts == null ? new ArrayList<>() : sorts;
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

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getHistogramBins() {
        return histogramBins;
    }

    public void setHistogramBins(Integer histogramBins) {
        this.histogramBins = histogramBins;
    }
}
