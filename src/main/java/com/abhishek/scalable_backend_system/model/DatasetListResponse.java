package com.abhishek.scalable_backend_system.model;

import java.util.List;

public class DatasetListResponse {

    private String batchId;
    private List<Dataset> datasets;

    public DatasetListResponse() {
    }

    public DatasetListResponse(String batchId, List<Dataset> datasets) {
        this.batchId = batchId;
        this.datasets = datasets;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public List<Dataset> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<Dataset> datasets) {
        this.datasets = datasets;
    }
}
