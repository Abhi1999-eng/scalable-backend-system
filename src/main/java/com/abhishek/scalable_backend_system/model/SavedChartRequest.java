package com.abhishek.scalable_backend_system.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public class SavedChartRequest {

    @NotBlank
    @Size(max = 255)
    private String name;

    @NotBlank
    private String chartType;

    @NotBlank
    private String xColumn;

    private String yColumn;

    @NotBlank
    private String aggregation;

    @Min(1)
    @Max(50)
    private int limit = 10;

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

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
