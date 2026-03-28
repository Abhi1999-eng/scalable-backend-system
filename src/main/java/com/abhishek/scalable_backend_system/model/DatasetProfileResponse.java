package com.abhishek.scalable_backend_system.model;

import java.util.List;

public class DatasetProfileResponse {

    private Dataset dataset;
    private List<DatasetColumnProfile> columns;

    public DatasetProfileResponse() {
    }

    public DatasetProfileResponse(Dataset dataset, List<DatasetColumnProfile> columns) {
        this.dataset = dataset;
        this.columns = columns;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public List<DatasetColumnProfile> getColumns() {
        return columns;
    }

    public void setColumns(List<DatasetColumnProfile> columns) {
        this.columns = columns;
    }
}
