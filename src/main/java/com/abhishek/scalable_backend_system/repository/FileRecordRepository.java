package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.StoredFileRecord;

import java.util.List;

public interface FileRecordRepository {

    void batchInsert(List<StoredFileRecord> records);

    List<StoredFileRecord> findByUploadId(String uploadId);
}
