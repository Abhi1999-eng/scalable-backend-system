package com.abhishek.scalable_backend_system.model;

public class DatasetUploadResponse {

    private String datasetId;
    private String message;

    public DatasetUploadResponse() {
    }

    public DatasetUploadResponse(String datasetId, String message) {
        this.datasetId = datasetId;
        this.message = message;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
