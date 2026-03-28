package com.abhishek.scalable_backend_system.model;

public class DatasetChartPoint {

    private String label;
    private Double x;
    private Double y;

    public DatasetChartPoint() {
    }

    public DatasetChartPoint(String label, Double x, Double y) {
        this.label = label;
        this.x = x;
        this.y = y;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }
}
