package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.model.FileUploadRequestResponse;
import com.abhishek.scalable_backend_system.model.FileUploadStatus;
import com.abhishek.scalable_backend_system.model.StoredFileRecord;
import com.abhishek.scalable_backend_system.service.FileIngestionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/ingestion/files")
public class FileIngestionController {

    private final FileIngestionService fileIngestionService;

    public FileIngestionController(FileIngestionService fileIngestionService) {
        this.fileIngestionService = fileIngestionService;
    }

    @PostMapping(
            value = "/bulk",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE
    )
    public ResponseEntity<FileUploadRequestResponse> uploadFiles(
            @RequestPart("files") List<MultipartFile> files) {

        String uploadId = fileIngestionService.submitMultipartUpload(files);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new FileUploadRequestResponse(
                        uploadId,
                        "Multipart upload accepted for async processing."
                ));
    }

    @PostMapping(
            value = "/zip",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE
    )
    public ResponseEntity<FileUploadRequestResponse> uploadArchive(
            @RequestPart("archive") MultipartFile archive) {

        String uploadId = fileIngestionService.submitZipUpload(archive);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new FileUploadRequestResponse(
                        uploadId,
                        "ZIP upload accepted for async processing."
                ));
    }

    @GetMapping("/{uploadId}")
    public ResponseEntity<FileUploadStatus> getUploadStatus(@PathVariable String uploadId) {
        FileUploadStatus status = fileIngestionService.getStatus(uploadId);

        if (status == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(status);
    }

    @GetMapping("/{uploadId}/records")
    public ResponseEntity<List<StoredFileRecord>> getUploadRecords(@PathVariable String uploadId) {
        return ResponseEntity.ok(fileIngestionService.getRecords(uploadId));
    }
}
