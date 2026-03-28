package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.StoredFileRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

@Repository
public class FileRecordJdbcRepository implements FileRecordRepository {

    private static final String INSERT_SQL = """
            INSERT INTO stored_files (
                upload_id,
                original_filename,
                stored_filename,
                storage_path,
                content_type,
                size_bytes,
                line_count,
                checksum_sha256,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    private static final String SELECT_BY_UPLOAD_ID_SQL = """
            SELECT
                id,
                upload_id,
                original_filename,
                stored_filename,
                storage_path,
                content_type,
                size_bytes,
                line_count,
                checksum_sha256,
                status,
                created_at
            FROM stored_files
            WHERE upload_id = ?
            ORDER BY created_at DESC, id DESC
            """;

    private final JdbcTemplate jdbcTemplate;

    public FileRecordJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void batchInsert(List<StoredFileRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(
                INSERT_SQL,
                records,
                records.size(),
                (PreparedStatement statement, StoredFileRecord record) -> {
                    statement.setString(1, record.getUploadId());
                    statement.setString(2, record.getOriginalFilename());
                    statement.setString(3, record.getStoredFilename());
                    statement.setString(4, record.getStoragePath());
                    statement.setString(5, record.getContentType());
                    statement.setLong(6, record.getSizeBytes());
                    statement.setLong(7, record.getLineCount());
                    statement.setString(8, record.getChecksumSha256());
                    statement.setString(9, record.getStatus());
                    statement.setTimestamp(10, Timestamp.from(record.getCreatedAt()));
                }
        );
    }

    @Override
    public List<StoredFileRecord> findByUploadId(String uploadId) {
        return jdbcTemplate.query(
                SELECT_BY_UPLOAD_ID_SQL,
                (resultSet, rowNum) -> {
                    StoredFileRecord record = new StoredFileRecord();
                    record.setId(resultSet.getLong("id"));
                    record.setUploadId(resultSet.getString("upload_id"));
                    record.setOriginalFilename(resultSet.getString("original_filename"));
                    record.setStoredFilename(resultSet.getString("stored_filename"));
                    record.setStoragePath(resultSet.getString("storage_path"));
                    record.setContentType(resultSet.getString("content_type"));
                    record.setSizeBytes(resultSet.getLong("size_bytes"));
                    record.setLineCount(resultSet.getLong("line_count"));
                    record.setChecksumSha256(resultSet.getString("checksum_sha256"));
                    record.setStatus(resultSet.getString("status"));
                    record.setCreatedAt(resultSet.getTimestamp("created_at").toInstant());
                    return record;
                },
                uploadId
        );
    }
}
