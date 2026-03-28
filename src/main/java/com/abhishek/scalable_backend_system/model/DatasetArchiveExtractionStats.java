package com.abhishek.scalable_backend_system.model;

public class DatasetArchiveExtractionStats {

    private int entriesDiscovered;
    private int csvFilesAccepted;
    private int skippedNonCsvEntries;

    public int getEntriesDiscovered() {
        return entriesDiscovered;
    }

    public void setEntriesDiscovered(int entriesDiscovered) {
        this.entriesDiscovered = entriesDiscovered;
    }

    public int getCsvFilesAccepted() {
        return csvFilesAccepted;
    }

    public void setCsvFilesAccepted(int csvFilesAccepted) {
        this.csvFilesAccepted = csvFilesAccepted;
    }

    public int getSkippedNonCsvEntries() {
        return skippedNonCsvEntries;
    }

    public void setSkippedNonCsvEntries(int skippedNonCsvEntries) {
        this.skippedNonCsvEntries = skippedNonCsvEntries;
    }
}
