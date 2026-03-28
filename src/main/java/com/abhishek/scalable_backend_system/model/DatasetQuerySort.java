package com.abhishek.scalable_backend_system.model;

import jakarta.validation.constraints.NotBlank;

public class DatasetQuerySort {

    @NotBlank(message = "sort.column is required")
    private String column;

    private String direction = "asc";

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }
}
