package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.FileUploadStatus;
import com.abhishek.scalable_backend_system.model.StoredFileRecord;
import com.abhishek.scalable_backend_system.repository.FileRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class FileIngestionService {

    private static final Logger log = LoggerFactory.getLogger(FileIngestionService.class);

    private static final int DB_BATCH_SIZE = 500;
    private static final int FILE_CHUNK_SIZE = 250;
    private static final int BUFFER_SIZE = 64 * 1024;

    private final FileRecordRepository fileRecordRepository;
    private final IngestionStatusService ingestionStatusService;
    private final Executor ingestionTaskExecutor;
    private final Executor ingestionWorkerExecutor;
    private final Path storageRoot;
    private final Path stagingRoot;

    public FileIngestionService(
            FileRecordRepository fileRecordRepository,
            IngestionStatusService ingestionStatusService,
            @Qualifier("ingestionTaskExecutor") Executor ingestionTaskExecutor,
            @Qualifier("ingestionWorkerExecutor") Executor ingestionWorkerExecutor,
            @Value("${app.ingestion.storage-root:uploads}") String storageRoot) {
        this.fileRecordRepository = fileRecordRepository;
        this.ingestionStatusService = ingestionStatusService;
        this.ingestionTaskExecutor = ingestionTaskExecutor;
        this.ingestionWorkerExecutor = ingestionWorkerExecutor;
        this.storageRoot = Paths.get(storageRoot).toAbsolutePath().normalize();
        this.stagingRoot = this.storageRoot.resolve(".staging");
    }

    public String submitMultipartUpload(List<MultipartFile> files) {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("At least one file is required.");
        }

        String uploadId = UUID.randomUUID().toString();
        ingestionStatusService.create(uploadId, files.size(), "Waiting for multipart ingestion to start.");
        ingestionStatusService.setCurrentStage(uploadId, "STAGING_MULTIPART");
        List<StagedUploadFile> stagedFiles = stageMultipartFiles(uploadId, files);
        ingestionTaskExecutor.execute(() -> processMultipartUpload(uploadId, stagedFiles));
        return uploadId;
    }

    public String submitZipUpload(MultipartFile archive) {
        if (archive == null || archive.isEmpty()) {
            throw new IllegalArgumentException("Archive must not be empty.");
        }

        String uploadId = UUID.randomUUID().toString();
        ingestionStatusService.create(uploadId, 0, "Waiting for ZIP ingestion to start.");
        ingestionStatusService.setCurrentStage(uploadId, "STAGING_ZIP");
        StagedUploadFile stagedArchive = stageArchive(uploadId, archive);
        ingestionTaskExecutor.execute(() -> processZipUpload(uploadId, stagedArchive));
        return uploadId;
    }

    public FileUploadStatus getStatus(String uploadId) {
        return ingestionStatusService.get(uploadId);
    }

    public List<StoredFileRecord> getRecords(String uploadId) {
        return fileRecordRepository.findByUploadId(uploadId);
    }

    void processMultipartUpload(String uploadId, List<StagedUploadFile> files) {
        try {
            Files.createDirectories(storageRoot.resolve(uploadId));
            ingestionStatusService.markStarted(uploadId);
            ingestionStatusService.setCurrentStage(uploadId, "PROCESSING_MULTIPART");

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int start = 0; start < files.size(); start += FILE_CHUNK_SIZE) {
                int end = Math.min(start + FILE_CHUNK_SIZE, files.size());
                List<StagedUploadFile> chunk = files.subList(start, end);

                futures.add(CompletableFuture.runAsync(
                        () -> processMultipartChunk(uploadId, chunk),
                        ingestionWorkerExecutor
                ));
            }

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            FileUploadStatus status = ingestionStatusService.get(uploadId);
            ingestionStatusService.markCompleted(
                    uploadId,
                    "Processed " + status.getProcessedFiles() + " files. Failed: " + status.getFailedFiles() + "."
            );
        } catch (Exception exception) {
            log.error("Multipart ingestion failed for upload {}", uploadId, exception);
            ingestionStatusService.markFailed(uploadId, "Multipart ingestion failed: " + exception.getMessage());
        } finally {
            cleanupStaging(uploadId);
        }
    }

    void processZipUpload(String uploadId, StagedUploadFile archive) {
        try {
            Files.createDirectories(storageRoot.resolve(uploadId));
            ingestionStatusService.markStarted(uploadId);
            ingestionStatusService.setCurrentStage(uploadId, "COUNTING_ZIP_ENTRIES");
            ingestionStatusService.setTotalFiles(uploadId, countZipEntries(archive));
            ingestionStatusService.setCurrentStage(uploadId, "PROCESSING_ZIP");

            List<StoredFileRecord> batch = new ArrayList<>(DB_BATCH_SIZE);

            try (ZipInputStream zipInputStream = new ZipInputStream(
                    new BufferedInputStream(Files.newInputStream(archive.path()), BUFFER_SIZE))) {
                ZipEntry entry;
                while ((entry = zipInputStream.getNextEntry()) != null) {
                    if (entry.isDirectory()) {
                        continue;
                    }

                    try {
                        batch.add(storeStream(
                                uploadId,
                                entry.getName(),
                                defaultContentType(entry.getName()),
                                zipInputStream
                        ));
                        if (batch.size() >= DB_BATCH_SIZE) {
                            fileRecordRepository.batchInsert(batch);
                            batch.clear();
                        }
                    } catch (Exception exception) {
                        log.warn("Failed to store ZIP entry {} for upload {}", entry.getName(), uploadId, exception);
                        ingestionStatusService.incrementFailed(uploadId);
                    } finally {
                        zipInputStream.closeEntry();
                    }
                }
            }

            fileRecordRepository.batchInsert(batch);
            FileUploadStatus status = ingestionStatusService.get(uploadId);
            ingestionStatusService.markCompleted(
                    uploadId,
                    "Processed " + status.getProcessedFiles() + " files from archive. Failed: " + status.getFailedFiles() + "."
            );
        } catch (Exception exception) {
            log.error("ZIP ingestion failed for upload {}", uploadId, exception);
            ingestionStatusService.markFailed(uploadId, "ZIP ingestion failed: " + exception.getMessage());
        } finally {
            cleanupStaging(uploadId);
        }
    }

    private void processMultipartChunk(String uploadId, List<StagedUploadFile> chunk) {
        List<StoredFileRecord> batch = new ArrayList<>(DB_BATCH_SIZE);

        for (StagedUploadFile file : chunk) {
            if (file == null || file.sizeBytes() == 0) {
                ingestionStatusService.incrementFailed(uploadId);
                continue;
            }

            try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(file.path()), BUFFER_SIZE)) {
                batch.add(storeStream(
                        uploadId,
                        safeOriginalName(file.originalFilename()),
                        file.contentType(),
                        inputStream
                ));
                if (batch.size() >= DB_BATCH_SIZE) {
                    fileRecordRepository.batchInsert(batch);
                    batch.clear();
                }
            } catch (Exception exception) {
                log.warn("Failed to store multipart file {} for upload {}", file.originalFilename(), uploadId, exception);
                ingestionStatusService.incrementFailed(uploadId);
            }
        }

        fileRecordRepository.batchInsert(batch);
    }

    private StoredFileRecord storeStream(
            String uploadId,
            String originalFilename,
            String contentType,
            InputStream inputStream) throws IOException, NoSuchAlgorithmException {

        String safeName = sanitizePath(originalFilename);
        Path uploadDirectory = storageRoot.resolve(uploadId);
        Path targetPath = resolveStoragePath(uploadDirectory, safeName);

        Files.createDirectories(targetPath.getParent());

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long totalBytes = 0;
        long lineCount = 0;
        int lastByte = -1;
        byte[] buffer = new byte[BUFFER_SIZE];

        try (OutputStream outputStream = Files.newOutputStream(
                targetPath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        )) {
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                digest.update(buffer, 0, bytesRead);
                totalBytes += bytesRead;
                for (int index = 0; index < bytesRead; index++) {
                    if (buffer[index] == '\n') {
                        lineCount++;
                    }
                }
                lastByte = buffer[bytesRead - 1];
            }
        }

        if (totalBytes > 0 && lastByte != '\n') {
            lineCount++;
        }

        StoredFileRecord record = new StoredFileRecord();
        record.setUploadId(uploadId);
        record.setOriginalFilename(safeName);
        record.setStoredFilename(targetPath.getFileName().toString());
        record.setStoragePath(targetPath.toString());
        record.setContentType(contentType == null ? defaultContentType(safeName) : contentType);
        record.setSizeBytes(totalBytes);
        record.setLineCount(lineCount);
        record.setChecksumSha256(toHex(digest.digest()));
        record.setStatus("STORED");
        record.setCreatedAt(Instant.now());

        ingestionStatusService.incrementProcessed(uploadId, totalBytes);
        return record;
    }

    private Path resolveStoragePath(Path uploadDirectory, String originalFilename) {
        String uniquePrefix = UUID.randomUUID().toString();
        Path normalized = uploadDirectory.resolve(uniquePrefix + "-" + originalFilename).normalize();

        if (!normalized.startsWith(uploadDirectory)) {
            throw new IllegalArgumentException("Invalid file path: " + originalFilename);
        }

        return normalized;
    }

    private int countZipEntries(StagedUploadFile archive) throws IOException {
        int count = 0;
        try (ZipInputStream zipInputStream = new ZipInputStream(
                new BufferedInputStream(Files.newInputStream(archive.path()), BUFFER_SIZE))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    count++;
                }
                zipInputStream.closeEntry();
            }
        }
        return count;
    }

    private List<StagedUploadFile> stageMultipartFiles(String uploadId, List<MultipartFile> files) {
        try {
            Path uploadStagingDirectory = stagingRoot.resolve(uploadId);
            Files.createDirectories(uploadStagingDirectory);

            List<StagedUploadFile> stagedFiles = new ArrayList<>(files.size());
            int index = 0;
            for (MultipartFile file : files) {
                if (file == null) {
                    continue;
                }

                String originalFilename = safeOriginalName(file.getOriginalFilename());
                Path stagedPath = uploadStagingDirectory.resolve(index + "-" + sanitizePath(originalFilename)).normalize();
                Files.createDirectories(stagedPath.getParent());

                try (InputStream inputStream = file.getInputStream()) {
                    Files.copy(inputStream, stagedPath);
                }

                stagedFiles.add(new StagedUploadFile(
                        originalFilename,
                        file.getContentType(),
                        stagedPath,
                        Files.size(stagedPath)
                ));
                index++;
            }

            return stagedFiles;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to stage multipart files.", exception);
        }
    }

    private StagedUploadFile stageArchive(String uploadId, MultipartFile archive) {
        try {
            Path uploadStagingDirectory = stagingRoot.resolve(uploadId);
            Files.createDirectories(uploadStagingDirectory);

            String fileName = safeOriginalName(archive.getOriginalFilename());
            Path stagedPath = uploadStagingDirectory.resolve("archive-" + sanitizePath(fileName)).normalize();
            Files.createDirectories(stagedPath.getParent());

            try (InputStream inputStream = archive.getInputStream()) {
                Files.copy(inputStream, stagedPath);
            }

            return new StagedUploadFile(
                    fileName,
                    archive.getContentType(),
                    stagedPath,
                    Files.size(stagedPath)
            );
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to stage ZIP archive.", exception);
        }
    }

    private void cleanupStaging(String uploadId) {
        Path uploadStagingDirectory = stagingRoot.resolve(uploadId);
        if (!Files.exists(uploadStagingDirectory)) {
            return;
        }

        try {
            Files.walk(uploadStagingDirectory)
                    .sorted((left, right) -> right.compareTo(left))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }

    private String safeOriginalName(String fileName) {
        return fileName == null || fileName.isBlank() ? "unnamed-file" : fileName;
    }

    private String sanitizePath(String originalFilename) {
        String normalized = originalFilename.replace('\\', '/');
        Path sanitized = Paths.get(normalized).normalize();

        if (sanitized.isAbsolute() || sanitized.startsWith("..")) {
            throw new IllegalArgumentException("File path is not allowed: " + originalFilename);
        }

        return sanitized.toString().replace("..", "");
    }

    private String defaultContentType(String fileName) {
        String detected = URLConnection.guessContentTypeFromName(fileName);
        return detected == null ? "application/octet-stream" : detected;
    }

    private String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            builder.append(String.format("%02x", value));
        }
        return builder.toString();
    }

    record StagedUploadFile(
            String originalFilename,
            String contentType,
            Path path,
            long sizeBytes
    ) {
    }
}
