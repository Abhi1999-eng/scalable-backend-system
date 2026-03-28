package com.abhishek.scalable_backend_system.model;

public class FileUploadRequestResponse {

    private String uploadId;
    private String message;

    public FileUploadRequestResponse() {
    }

    public FileUploadRequestResponse(String uploadId, String message) {
        this.uploadId = uploadId;
        this.message = message;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
