package com.abhishek.scalable_backend_system.model;

import jakarta.validation.constraints.NotBlank;

public class DatasetQueryFilter {

    @NotBlank(message = "filter.column is required")
    private String column;

    @NotBlank(message = "filter.operator is required")
    private String operator;

    private String value;

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
