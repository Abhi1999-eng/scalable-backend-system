package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.model.Dataset;
import com.abhishek.scalable_backend_system.model.DatasetBatchProgressResponse;
import com.abhishek.scalable_backend_system.model.DatasetChartRequest;
import com.abhishek.scalable_backend_system.model.DatasetChartResponse;
import com.abhishek.scalable_backend_system.model.DatasetColumnProfile;
import com.abhishek.scalable_backend_system.model.DatasetDashboardItem;
import com.abhishek.scalable_backend_system.model.DatasetJob;
import com.abhishek.scalable_backend_system.model.DatasetListResponse;
import com.abhishek.scalable_backend_system.model.DatasetProfileResponse;
import com.abhishek.scalable_backend_system.model.DatasetQueryRequest;
import com.abhishek.scalable_backend_system.model.DatasetQueryResponse;
import com.abhishek.scalable_backend_system.model.SavedChart;
import com.abhishek.scalable_backend_system.model.SavedChartRequest;
import com.abhishek.scalable_backend_system.service.DatasetService;
import com.abhishek.scalable_backend_system.service.RequestAuthService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@Validated
@RestController
@RequestMapping("/datasets")
public class DatasetController {

    private final DatasetService datasetService;
    private final RequestAuthService requestAuthService;

    public DatasetController(DatasetService datasetService, RequestAuthService requestAuthService) {
        this.datasetService = datasetService;
        this.requestAuthService = requestAuthService;
    }

    @PostMapping(
            value = "/upload",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE
    )
    public ResponseEntity<DatasetListResponse> uploadDataset(
            HttpServletRequest servletRequest,
            @RequestPart(value = "files", required = false) List<MultipartFile> files,
            @RequestPart(value = "archives", required = false) List<MultipartFile> archives) {
        DatasetListResponse response = datasetService.submitDatasets(
                requestAuthService.requireCurrentUserId(servletRequest),
                files,
                archives
        );
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping
    public ResponseEntity<List<DatasetDashboardItem>> getDatasetDashboard(
            HttpServletRequest servletRequest,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return ResponseEntity.ok(datasetService.getDatasetDashboard(
                requestAuthService.requireCurrentUserId(servletRequest),
                page,
                size
        ));
    }

    @GetMapping("/batches/{batchId}")
    public ResponseEntity<DatasetListResponse> getBatch(@PathVariable String batchId, HttpServletRequest servletRequest) {
        List<Dataset> datasets = datasetService.getBatchDatasets(batchId, requestAuthService.requireCurrentUserId(servletRequest));
        if (datasets.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new DatasetListResponse(batchId, datasets));
    }

    @GetMapping("/batches/{batchId}/progress")
    public ResponseEntity<DatasetBatchProgressResponse> getBatchProgress(@PathVariable String batchId, HttpServletRequest servletRequest) {
        return ResponseEntity.ok(datasetService.getBatchProgress(batchId, requestAuthService.requireCurrentUserId(servletRequest)));
    }

    @GetMapping("/{datasetId}/status")
    public ResponseEntity<Dataset> getStatus(@PathVariable String datasetId, HttpServletRequest servletRequest) {
        return datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{datasetId}/job")
    public ResponseEntity<DatasetJob> getJob(@PathVariable String datasetId, HttpServletRequest servletRequest) {
        return datasetService.getDatasetJob(datasetId, requestAuthService.requireCurrentUserId(servletRequest))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{datasetId}/jobs/retry")
    public ResponseEntity<DatasetJob> retryJob(@PathVariable String datasetId, HttpServletRequest servletRequest) {
        return ResponseEntity.accepted().body(
                datasetService.retryDatasetJob(datasetId, requestAuthService.requireCurrentUserId(servletRequest))
        );
    }

    @GetMapping("/{datasetId}/schema")
    public ResponseEntity<List<DatasetColumnProfile>> getSchema(@PathVariable String datasetId, HttpServletRequest servletRequest) {
        if (datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest)).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(datasetService.getSchema(datasetId));
    }

    @GetMapping("/{datasetId}/profile")
    public ResponseEntity<DatasetProfileResponse> getProfile(@PathVariable String datasetId, HttpServletRequest servletRequest) {
        return datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest))
                .map(dataset -> ResponseEntity.ok(
                        new DatasetProfileResponse(dataset, datasetService.getSchema(datasetId))
                ))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{datasetId}/preview")
    public ResponseEntity<List<Map<String, Object>>> getPreview(
            HttpServletRequest servletRequest,
            @PathVariable String datasetId,
            @RequestParam(defaultValue = "25") @Min(1) @Max(200) int limit) {
        if (datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest)).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(datasetService.getPreview(datasetId, limit));
    }

    @GetMapping("/{datasetId}/rows")
    public ResponseEntity<List<Map<String, Object>>> getRows(
            HttpServletRequest servletRequest,
            @PathVariable String datasetId,
            @RequestParam(defaultValue = "25") @Min(1) @Max(500) int limit,
            @RequestParam(defaultValue = "0") @Min(0) int offset) {
        if (datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest)).isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(datasetService.getRows(datasetId, limit, offset));
    }

    @PostMapping("/{datasetId}/query")
    public ResponseEntity<DatasetQueryResponse> queryDataset(
            HttpServletRequest servletRequest,
            @PathVariable String datasetId,
            @Valid @RequestBody DatasetQueryRequest queryRequest) {
        if (datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest)).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(datasetService.queryDataset(datasetId, queryRequest));
    }

    @PostMapping("/{datasetId}/charts")
    public ResponseEntity<DatasetChartResponse> generateChart(
            HttpServletRequest servletRequest,
            @PathVariable String datasetId,
            @RequestBody Map<String, Object> payload) {
        if (datasetService.getDataset(datasetId, requestAuthService.requireCurrentUserId(servletRequest)).isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        DatasetChartRequest request = new DatasetChartRequest();
        request.setChartType(asString(payload.get("chartType")));
        request.setXColumn(asString(payload.get("xColumn")));
        request.setYColumn(asString(payload.get("yColumn")));
        request.setAggregation(asString(payload.get("aggregation")));
        request.setLimit(asInteger(payload.get("limit")));

        return ResponseEntity.ok(datasetService.buildChart(datasetId, request));
    }

    @GetMapping("/{datasetId}/saved-charts")
    public ResponseEntity<List<SavedChart>> getSavedCharts(@PathVariable String datasetId, HttpServletRequest servletRequest) {
        return ResponseEntity.ok(datasetService.getSavedCharts(datasetId, requestAuthService.requireCurrentUserId(servletRequest)));
    }

    @PostMapping("/{datasetId}/saved-charts")
    public ResponseEntity<SavedChart> saveChart(
            @PathVariable String datasetId,
            HttpServletRequest servletRequest,
            @Valid @RequestBody SavedChartRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(
                datasetService.saveChart(datasetId, requestAuthService.requireCurrentUserId(servletRequest), request)
        );
    }

    private String asString(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException exception) {
            return null;
        }
    }
}
