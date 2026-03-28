package com.abhishek.scalable_backend_system.model;

public class DatasetChartRequest {

    private String chartType;
    private String xColumn;
    private String yColumn;
    private String aggregation;
    private Integer limit;

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

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}
