package com.abhishek.scalable_backend_system.model;

import java.util.List;

public class DatasetChartResponse {

    private String chartType;
    private String datasetId;
    private String xColumn;
    private String yColumn;
    private String aggregation;
    private List<String> labels;
    private List<Double> values;
    private List<DatasetChartPoint> points;
    private DatasetBoxPlotStats boxPlot;

    public String getChartType() {
        return chartType;
    }

    public void setChartType(String chartType) {
        this.chartType = chartType;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
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

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

    public List<Double> getValues() {
        return values;
    }

    public void setValues(List<Double> values) {
        this.values = values;
    }

    public List<DatasetChartPoint> getPoints() {
        return points;
    }

    public void setPoints(List<DatasetChartPoint> points) {
        this.points = points;
    }

    public DatasetBoxPlotStats getBoxPlot() {
        return boxPlot;
    }

    public void setBoxPlot(DatasetBoxPlotStats boxPlot) {
        this.boxPlot = boxPlot;
    }
}
