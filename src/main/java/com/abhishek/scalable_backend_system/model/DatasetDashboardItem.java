package com.abhishek.scalable_backend_system.model;

public class DatasetDashboardItem {

    private Dataset dataset;
    private DatasetJob job;

    public DatasetDashboardItem() {
    }

    public DatasetDashboardItem(Dataset dataset, DatasetJob job) {
        this.dataset = dataset;
        this.job = job;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public DatasetJob getJob() {
        return job;
    }

    public void setJob(DatasetJob job) {
        this.job = job;
    }
}
