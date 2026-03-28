package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.FileUploadStatus;
import com.abhishek.scalable_backend_system.model.StoredFileRecord;
import com.abhishek.scalable_backend_system.repository.FileRecordRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileIngestionServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldStoreMultipartFilesAndTrackStatus() {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        IngestionStatusService statusService = new IngestionStatusService();
        Executor directExecutor = Runnable::run;

        FileIngestionService service = new FileIngestionService(
                repository,
                statusService,
                directExecutor,
                directExecutor,
                tempDir.toString()
        );

        List<MockMultipartFile> files = List.of(
                new MockMultipartFile("files", "alpha.txt", "text/plain", "hello\nworld\n".getBytes(StandardCharsets.UTF_8)),
                new MockMultipartFile("files", "beta.txt", "text/plain", "spring".getBytes(StandardCharsets.UTF_8))
        );

        String uploadId = service.submitMultipartUpload(new ArrayList<>(files));
        FileUploadStatus status = service.getStatus(uploadId);

        assertEquals("COMPLETED", status.getState());
        assertEquals(2, status.getProcessedFiles());
        assertEquals(0, status.getFailedFiles());
        assertEquals(2, repository.records.size());
        assertEquals(18, status.getStoredBytes());
        assertEquals(2, repository.records.get(0).getLineCount());
        assertEquals(1, repository.records.get(1).getLineCount());
    }

    @Test
    void shouldStoreZipEntriesAndTrackStatus() throws IOException {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        IngestionStatusService statusService = new IngestionStatusService();
        Executor directExecutor = Runnable::run;

        FileIngestionService service = new FileIngestionService(
                repository,
                statusService,
                directExecutor,
                directExecutor,
                tempDir.toString()
        );

        MockMultipartFile archive = new MockMultipartFile(
                "archive",
                "files.zip",
                "application/zip",
                zipBytes("nested/one.txt", "one\n", "nested/two.txt", "two")
        );

        String uploadId = service.submitZipUpload(archive);
        FileUploadStatus status = service.getStatus(uploadId);

        assertEquals("COMPLETED", status.getState());
        assertEquals(2, status.getProcessedFiles());
        assertEquals(2, status.getTotalFiles());
        assertEquals(2, repository.records.size());
        assertNotNull(repository.records.get(0).getChecksumSha256());
        assertTrue(Files.exists(Path.of(repository.records.get(0).getStoragePath())));
        assertTrue(repository.records.stream().allMatch(record -> record.getStoragePath().startsWith(tempDir.toString())));
    }

    @Test
    void shouldReturnStoredRecordsForUploadId() {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        FileIngestionService service = createService(repository);

        List<MockMultipartFile> files = List.of(
                new MockMultipartFile("files", "alpha.txt", "text/plain", "alpha".getBytes(StandardCharsets.UTF_8)),
                new MockMultipartFile("files", "beta.txt", "text/plain", "beta".getBytes(StandardCharsets.UTF_8))
        );

        String uploadId = service.submitMultipartUpload(new ArrayList<>(files));

        assertEquals(2, service.getRecords(uploadId).size());
    }

    @Test
    void shouldCleanStagingFilesAfterZipProcessing() throws IOException {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        FileIngestionService service = createService(repository);

        MockMultipartFile archive = new MockMultipartFile(
                "archive",
                "files.zip",
                "application/zip",
                zipBytes("one.txt", "one", "two.txt", "two")
        );

        String uploadId = service.submitZipUpload(archive);

        assertEquals("COMPLETED", service.getStatus(uploadId).getState());
        assertFalse(Files.exists(tempDir.resolve(".staging").resolve(uploadId)));
    }

    @Test
    void shouldRejectEmptyMultipartUpload() {
        FileIngestionService service = createService(new InMemoryFileRecordRepository());

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.submitMultipartUpload(List.of())
        );

        assertEquals("At least one file is required.", exception.getMessage());
    }

    @Test
    void shouldRejectEmptyZipUpload() {
        FileIngestionService service = createService(new InMemoryFileRecordRepository());
        MockMultipartFile archive = new MockMultipartFile("archive", "empty.zip", "application/zip", new byte[0]);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.submitZipUpload(archive)
        );

        assertEquals("Archive must not be empty.", exception.getMessage());
    }

    @Test
    void shouldContinueWhenMultipartBatchContainsEmptyFiles() {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        FileIngestionService service = createService(repository);

        List<MockMultipartFile> files = List.of(
                new MockMultipartFile("files", "alpha.txt", "text/plain", "alpha".getBytes(StandardCharsets.UTF_8)),
                new MockMultipartFile("files", "empty.txt", "text/plain", new byte[0]),
                new MockMultipartFile("files", "beta.txt", "text/plain", "beta".getBytes(StandardCharsets.UTF_8))
        );

        String uploadId = service.submitMultipartUpload(new ArrayList<>(files));
        FileUploadStatus status = service.getStatus(uploadId);

        assertAll(
                () -> assertEquals("COMPLETED", status.getState()),
                () -> assertEquals(2, status.getProcessedFiles()),
                () -> assertEquals(1, status.getFailedFiles()),
                () -> assertEquals(2, repository.records.size())
        );
    }

    @Test
    void shouldRejectZipSlipPathsAndKeepOtherEntries() throws IOException {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        FileIngestionService service = createService(repository);

        MockMultipartFile archive = new MockMultipartFile(
                "archive",
                "files.zip",
                "application/zip",
                zipEntries(
                        new ZipFileSpec("../evil.txt", "bad"),
                        new ZipFileSpec("safe/good.txt", "good\n")
                )
        );

        String uploadId = service.submitZipUpload(archive);
        FileUploadStatus status = service.getStatus(uploadId);

        assertAll(
                () -> assertEquals("COMPLETED", status.getState()),
                () -> assertEquals(2, status.getTotalFiles()),
                () -> assertEquals(1, status.getProcessedFiles()),
                () -> assertEquals(1, status.getFailedFiles()),
                () -> assertEquals(1, repository.records.size()),
                () -> assertFalse(repository.records.get(0).getStoragePath().contains(".."))
        );
    }

    @Test
    void shouldProcessLargeMultipartBatchAcrossChunks() {
        InMemoryFileRecordRepository repository = new InMemoryFileRecordRepository();
        FileIngestionService service = createService(repository);

        List<MultipartFile> files = new ArrayList<>();
        long expectedBytes = 0;
        for (int index = 0; index < 600; index++) {
            String content = "line-" + index + "\n";
            expectedBytes += content.getBytes(StandardCharsets.UTF_8).length;
            files.add(new MockMultipartFile(
                    "files",
                    "file-" + index + ".txt",
                    "text/plain",
                    content.getBytes(StandardCharsets.UTF_8)
                ));
        }
        final long expectedStoredBytes = expectedBytes;

        String uploadId = service.submitMultipartUpload(files);
        FileUploadStatus status = service.getStatus(uploadId);

        assertAll(
                () -> assertEquals("COMPLETED", status.getState()),
                () -> assertEquals(600, status.getProcessedFiles()),
                () -> assertEquals(0, status.getFailedFiles()),
                () -> assertEquals(600, repository.records.size()),
                () -> assertEquals(expectedStoredBytes, status.getStoredBytes())
        );
    }

    @Test
    void shouldReturnNullForUnknownUploadStatus() {
        FileIngestionService service = createService(new InMemoryFileRecordRepository());

        assertNull(service.getStatus("missing-upload"));
    }

    private FileIngestionService createService(InMemoryFileRecordRepository repository) {
        IngestionStatusService statusService = new IngestionStatusService();
        Executor directExecutor = Runnable::run;

        return new FileIngestionService(
                repository,
                statusService,
                directExecutor,
                directExecutor,
                tempDir.toString()
        );
    }

    private byte[] zipBytes(String firstName, String firstContent, String secondName, String secondContent) throws IOException {
        return zipEntries(
                new ZipFileSpec(firstName, firstContent),
                new ZipFileSpec(secondName, secondContent)
        );
    }

    private byte[] zipEntries(ZipFileSpec... entries) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream)) {
            for (ZipFileSpec entry : entries) {
                zipOutputStream.putNextEntry(new ZipEntry(entry.name()));
                zipOutputStream.write(entry.content().getBytes(StandardCharsets.UTF_8));
                zipOutputStream.closeEntry();
            }
        }
        return outputStream.toByteArray();
    }

    record ZipFileSpec(String name, String content) {
    }

    private static class InMemoryFileRecordRepository implements FileRecordRepository {
        private final List<StoredFileRecord> records = new ArrayList<>();

        @Override
        public void batchInsert(List<StoredFileRecord> records) {
            this.records.addAll(records);
        }

        @Override
        public List<StoredFileRecord> findByUploadId(String uploadId) {
            return records.stream()
                    .filter(record -> uploadId.equals(record.getUploadId()))
                    .toList();
        }
    }
}
