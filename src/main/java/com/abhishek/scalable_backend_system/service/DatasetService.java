package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.Dataset;
import com.abhishek.scalable_backend_system.model.DatasetArchiveExtractionStats;
import com.abhishek.scalable_backend_system.model.DatasetBatchProgressResponse;
import com.abhishek.scalable_backend_system.model.DatasetBoxPlotStats;
import com.abhishek.scalable_backend_system.model.DatasetChartRequest;
import com.abhishek.scalable_backend_system.model.DatasetChartPoint;
import com.abhishek.scalable_backend_system.model.DatasetChartResponse;
import com.abhishek.scalable_backend_system.model.DatasetColumnProfile;
import com.abhishek.scalable_backend_system.model.DatasetDashboardItem;
import com.abhishek.scalable_backend_system.model.DatasetJob;
import com.abhishek.scalable_backend_system.model.DatasetListResponse;
import com.abhishek.scalable_backend_system.model.DatasetQueryFilter;
import com.abhishek.scalable_backend_system.model.DatasetQueryRequest;
import com.abhishek.scalable_backend_system.model.DatasetQueryResponse;
import com.abhishek.scalable_backend_system.model.DatasetQuerySort;
import com.abhishek.scalable_backend_system.model.DatasetRow;
import com.abhishek.scalable_backend_system.model.SavedChart;
import com.abhishek.scalable_backend_system.model.SavedChartRequest;
import com.abhishek.scalable_backend_system.repository.DatasetColumnProfileRepository;
import com.abhishek.scalable_backend_system.repository.DatasetRepository;
import com.abhishek.scalable_backend_system.repository.DatasetRowRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.Reader;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class DatasetService {

    private static final Logger log = LoggerFactory.getLogger(DatasetService.class);
    private static final int ROW_BATCH_SIZE = 500;

    private final DatasetRepository datasetRepository;
    private final DatasetColumnProfileRepository datasetColumnProfileRepository;
    private final DatasetRowRepository datasetRowRepository;
    private final ObjectMapper objectMapper;
    private final Executor ingestionTaskExecutor;
    private final DatasetAggregationService aggregationService;
    private final DatasetJobService datasetJobService;
    private final StringRedisTemplate redisTemplate;
    private final SavedChartService savedChartService;
    private final Path datasetRoot;
    private final long cacheTtlSeconds;
    private final int maxArchiveEntries;
    private final long maxArchiveUncompressedBytes;
    private final long slowIngestionWarnThresholdMs;

    @Autowired
    public DatasetService(
            DatasetRepository datasetRepository,
            DatasetColumnProfileRepository datasetColumnProfileRepository,
            DatasetRowRepository datasetRowRepository,
            ObjectMapper objectMapper,
            @Qualifier("ingestionTaskExecutor") Executor ingestionTaskExecutor,
            DatasetAggregationService aggregationService,
            DatasetJobService datasetJobService,
            StringRedisTemplate redisTemplate,
            SavedChartService savedChartService,
            @Value("${app.datasets.storage-root:dataset-uploads}") String datasetRoot,
            @Value("${app.dataset-query.cache-ttl-seconds:300}") long cacheTtlSeconds,
            @Value("${app.datasets.archive-max-entries:250}") int maxArchiveEntries,
            @Value("${app.datasets.archive-max-uncompressed-bytes:1073741824}") long maxArchiveUncompressedBytes,
            @Value("${app.monitoring.dataset-slow-threshold-ms:10000}") long slowIngestionWarnThresholdMs) {
        this.datasetRepository = datasetRepository;
        this.datasetColumnProfileRepository = datasetColumnProfileRepository;
        this.datasetRowRepository = datasetRowRepository;
        this.objectMapper = objectMapper;
        this.ingestionTaskExecutor = ingestionTaskExecutor;
        this.aggregationService = aggregationService;
        this.datasetJobService = datasetJobService;
        this.redisTemplate = redisTemplate;
        this.savedChartService = savedChartService;
        this.datasetRoot = Paths.get(datasetRoot).toAbsolutePath().normalize();
        this.cacheTtlSeconds = cacheTtlSeconds;
        this.maxArchiveEntries = maxArchiveEntries;
        this.maxArchiveUncompressedBytes = maxArchiveUncompressedBytes;
        this.slowIngestionWarnThresholdMs = slowIngestionWarnThresholdMs;
    }

    DatasetService(
            DatasetRepository datasetRepository,
            DatasetColumnProfileRepository datasetColumnProfileRepository,
            DatasetRowRepository datasetRowRepository,
            ObjectMapper objectMapper,
            Executor ingestionTaskExecutor,
            String datasetRoot) {
        this(
                datasetRepository,
                datasetColumnProfileRepository,
                datasetRowRepository,
                objectMapper,
                ingestionTaskExecutor,
                new DatasetAggregationService(),
                null,
                null,
                null,
                datasetRoot,
                300,
                250,
                1_073_741_824L,
                10_000L
        );
    }

    public DatasetListResponse submitDatasets(Long ownerUserId, List<MultipartFile> files, List<MultipartFile> archives) {
        List<MultipartFile> safeFiles = files == null ? List.of() : files;
        List<MultipartFile> safeArchives = archives == null ? List.of() : archives;

        if (safeFiles.isEmpty() && safeArchives.isEmpty()) {
            throw new IllegalArgumentException("Upload at least one CSV file or ZIP archive.");
        }

        String batchId = UUID.randomUUID().toString();
        List<Dataset> createdDatasets = new ArrayList<>();

        for (MultipartFile file : safeFiles) {
            if (file == null || file.isEmpty()) {
                continue;
            }
            createdDatasets.add(createDatasetFromMultipart(ownerUserId, batchId, file));
        }

        for (MultipartFile archive : safeArchives) {
            if (archive == null || archive.isEmpty()) {
                continue;
            }
            createdDatasets.addAll(extractDatasetsFromArchive(ownerUserId, batchId, archive));
        }

        if (createdDatasets.isEmpty()) {
            throw new IllegalArgumentException("No CSV files were found in the upload.");
        }

        createdDatasets.forEach(dataset -> {
            if (datasetJobService != null) {
                datasetJobService.createQueuedJob(dataset);
            }
            ingestionTaskExecutor.execute(() -> ingestDataset(dataset.getDatasetId()));
        });
        return new DatasetListResponse(batchId, createdDatasets);
    }

    public String submitDataset(Long ownerUserId, MultipartFile file) {
        return submitDatasets(ownerUserId, List.of(file), List.of()).getDatasets().get(0).getDatasetId();
    }

    public String submitDataset(MultipartFile file) {
        return submitDataset(null, file);
    }

    public Optional<Dataset> getDataset(String datasetId, Long ownerUserId) {
        if (ownerUserId == null) {
            return datasetRepository.findByDatasetId(datasetId);
        }
        return datasetRepository.findByDatasetIdAndOwnerUserId(datasetId, ownerUserId);
    }

    public Optional<Dataset> getDataset(String datasetId) {
        return getDataset(datasetId, null);
    }

    public List<Dataset> getBatchDatasets(String batchId, Long ownerUserId) {
        return datasetRepository.findByBatchIdAndOwnerUserIdOrderByCreatedAtAsc(batchId, ownerUserId);
    }

    public DatasetBatchProgressResponse getBatchProgress(String batchId, Long ownerUserId) {
        List<Dataset> datasets = getBatchDatasets(batchId, ownerUserId);
        if (datasets.isEmpty()) {
            throw new IllegalArgumentException("Batch not found.");
        }

        Map<String, DatasetJob> jobsByDatasetId = datasetJobService == null
                ? Map.of()
                : datasetJobService.getBatchJobs(batchId)
                .stream()
                .collect(LinkedHashMap::new, (map, job) -> map.put(job.getDatasetId(), job), Map::putAll);

        List<DatasetDashboardItem> items = datasets.stream()
                .map(dataset -> new DatasetDashboardItem(dataset, jobsByDatasetId.get(dataset.getDatasetId())))
                .toList();

        int queuedCount = 0;
        int processingCount = 0;
        int completedCount = 0;
        int failedCount = 0;
        int progressAccumulator = 0;
        long processedRows = 0;
        long totalRows = 0;

        for (DatasetDashboardItem item : items) {
            Dataset dataset = item.getDataset();
            DatasetJob job = item.getJob();
            String status = job != null ? job.getStatus() : dataset.getStatus();
            int progress = job != null ? job.getProgressPercentage() : ("READY".equalsIgnoreCase(status) ? 100 : 0);

            switch (status == null ? "" : status.toUpperCase(Locale.ROOT)) {
                case "COMPLETED", "READY" -> completedCount++;
                case "FAILED" -> failedCount++;
                case "PROCESSING" -> processingCount++;
                default -> queuedCount++;
            }

            progressAccumulator += progress;
            processedRows += job != null ? job.getProcessedRows() : dataset.getRowCount();
            totalRows += job != null && job.getTotalRows() > 0 ? job.getTotalRows() : dataset.getRowCount();
        }

        DatasetBatchProgressResponse response = new DatasetBatchProgressResponse();
        response.setBatchId(batchId);
        response.setDatasetCount(items.size());
        response.setQueuedCount(queuedCount);
        response.setProcessingCount(processingCount);
        response.setCompletedCount(completedCount);
        response.setFailedCount(failedCount);
        response.setProgressPercentage(items.isEmpty() ? 0 : Math.round(progressAccumulator / (float) items.size()));
        response.setProcessedRows(processedRows);
        response.setTotalRows(totalRows);
        response.setItems(items);
        return response;
    }

    public List<DatasetDashboardItem> getDatasetDashboard(Long ownerUserId, int page, int size) {
        Pageable pageable = PageRequest.of(Math.max(page, 0), Math.max(1, Math.min(size, 100)));
        return datasetRepository.findByOwnerUserIdOrderByCreatedAtDesc(ownerUserId, pageable)
                .stream()
                .map(dataset -> new DatasetDashboardItem(
                        dataset,
                        datasetJobService == null ? null : datasetJobService.getJob(dataset.getDatasetId()).orElse(null)
                ))
                .toList();
    }

    public Optional<DatasetJob> getDatasetJob(String datasetId, Long ownerUserId) {
        if (getDataset(datasetId, ownerUserId).isEmpty()) {
            return Optional.empty();
        }
        return datasetJobService == null ? Optional.empty() : datasetJobService.getJob(datasetId);
    }

    public DatasetJob retryDatasetJob(String datasetId, Long ownerUserId) {
        Dataset dataset = datasetRepository.findByDatasetIdAndOwnerUserId(datasetId, ownerUserId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found."));

        if (datasetJobService == null) {
            throw new IllegalArgumentException("Dataset jobs are not enabled.");
        }

        DatasetJob job = datasetJobService.retry(datasetId);
        dataset.setStatus("QUEUED");
        dataset.setErrorMessage(null);
        dataset.setStartedAt(null);
        dataset.setFinishedAt(null);
        datasetRepository.save(dataset);
        clearDatasetCaches(datasetId);
        ingestionTaskExecutor.execute(() -> ingestDataset(datasetId));
        return job;
    }

    public List<DatasetColumnProfile> getSchema(String datasetId) {
        return datasetColumnProfileRepository.findByDatasetIdOrderByColumnOrderIndexAsc(datasetId);
    }

    public List<SavedChart> getSavedCharts(String datasetId, Long ownerUserId) {
        requireOwnedDataset(datasetId, ownerUserId);
        return savedChartService.listForDataset(datasetId, ownerUserId);
    }

    public SavedChart saveChart(String datasetId, Long ownerUserId, SavedChartRequest request) {
        Dataset dataset = requireOwnedDataset(datasetId, ownerUserId);
        return savedChartService.save(dataset, ownerUserId, request);
    }

    public void deleteSavedChart(String chartId, Long ownerUserId) {
        savedChartService.delete(chartId, ownerUserId);
    }

    public List<Map<String, Object>> getPreview(String datasetId, int limit) {
        return getRows(datasetId, limit, 0);
    }

    public List<Map<String, Object>> getRows(String datasetId, int limit, int offset) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        int safeOffset = Math.max(offset, 0);
        int page = safeOffset / safeLimit;

        return datasetRowRepository.findByDatasetIdOrderByRowNumberAsc(datasetId, PageRequest.of(page, safeLimit))
                .stream()
                .map(DatasetRow::getRowJson)
                .map(this::readRowJson)
                .toList();
    }

    public DatasetQueryResponse queryDataset(String datasetId, DatasetQueryRequest request) {
        String cacheKey = buildQueryCacheKey(datasetId, request);
        DatasetQueryResponse cached = readCache(cacheKey, DatasetQueryResponse.class);
        if (cached != null) {
            return cached;
        }

        List<DatasetColumnProfile> schema = getSchema(datasetId);
        Map<String, DatasetColumnProfile> profileByColumn = schema.stream()
                .collect(LinkedHashMap::new, (map, column) -> map.put(column.getColumnName(), column), Map::putAll);
        List<Map<String, Object>> rows = datasetRowRepository.findByDatasetId(datasetId)
                .stream()
                .map(DatasetRow::getRowJson)
                .map(this::readRowJson)
                .toList();

        List<Map<String, Object>> filteredRows = rows.stream()
                .filter(row -> matchesAllFilters(row, request.getFilters(), profileByColumn))
                .toList();

        List<Map<String, Object>> sortedRows = sortRows(filteredRows, request.getSorts(), profileByColumn);

        DatasetQueryResponse response = new DatasetQueryResponse();
        response.setDatasetId(datasetId);
        response.setAggregation(normalizeAggregation(request.getAggregation()));
        response.setGroupByColumn(blankToNull(request.getGroupByColumn()));
        response.setAggregateColumn(blankToNull(request.getAggregateColumn()));
        response.setTotalRows(sortedRows.size());
        response.setLimit(safeLimit(request.getLimit()));
        response.setOffset(safeOffset(request.getOffset()));

        if ("histogram".equalsIgnoreCase(request.getAggregation())) {
            String sourceColumn = requireColumn(request.getAggregateColumn(), "aggregateColumn");
            validateColumnExists(profileByColumn, sourceColumn);
            response.setAggregates(aggregationService.histogram(sortedRows, sourceColumn, request.getHistogramBins() == null ? 10 : request.getHistogramBins()));
            response.setRows(List.of());
        } else if (request.getGroupByColumn() != null && !request.getGroupByColumn().isBlank()) {
            validateColumnExists(profileByColumn, request.getGroupByColumn());
            if (!"count".equalsIgnoreCase(request.getAggregation())) {
                String aggregateColumn = requireColumn(request.getAggregateColumn(), "aggregateColumn");
                validateColumnExists(profileByColumn, aggregateColumn);
            }
            response.setAggregates(aggregationService.aggregate(
                    sortedRows,
                    request.getGroupByColumn(),
                    request.getAggregateColumn(),
                    normalizeAggregation(request.getAggregation()),
                    safeLimit(request.getLimit())
            ));
            response.setRows(List.of());
        } else {
            int offset = safeOffset(request.getOffset());
            int limit = safeLimit(request.getLimit());
            response.setRows(sortedRows.stream().skip(offset).limit(limit).toList());
            response.setAggregates(List.of());
        }

        writeCache(cacheKey, response);
        return response;
    }

    public DatasetChartResponse buildChart(String datasetId, DatasetChartRequest request) {
        String chartCacheKey = buildChartCacheKey(datasetId, request);
        DatasetChartResponse cached = readCache(chartCacheKey, DatasetChartResponse.class);
        if (cached != null) {
            return cached;
        }

        String chartType = normalizeChartType(request.getChartType());
        String aggregation = normalizeAggregation(request.getAggregation());
        int limit = request.getLimit() == null ? 8 : Math.max(1, Math.min(request.getLimit(), 20));

        List<Map<String, Object>> rows = datasetRowRepository.findByDatasetId(datasetId)
                .stream()
                .map(DatasetRow::getRowJson)
                .map(this::readRowJson)
                .toList();

        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Dataset has no ingested rows yet.");
        }

        String xColumn = requireColumn(request.getXColumn(), "xColumn");
        DatasetChartResponse response = switch (chartType.toLowerCase(Locale.ROOT)) {
            case "histogram" -> buildHistogram(datasetId, xColumn, request.getYColumn(), limit, rows);
            case "scatter" -> buildScatter(datasetId, xColumn, request.getYColumn(), limit, rows);
            case "box", "boxplot", "whisker" -> buildBoxPlot(datasetId, xColumn, request.getYColumn(), rows);
            case "line", "area" -> buildOrderedAggregate(datasetId, chartType, xColumn, request.getYColumn(), aggregation, limit, rows);
            case "pie", "pictograph", "bar" -> buildCategoryAggregate(datasetId, chartType, xColumn, request.getYColumn(), aggregation, limit, rows);
            default -> buildCategoryAggregate(datasetId, "bar", xColumn, request.getYColumn(), aggregation, limit, rows);
        };
        writeCache(chartCacheKey, response);
        return response;
    }

    void ingestDataset(String datasetId) {
        Dataset dataset = datasetRepository.findByDatasetId(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + datasetId));
        long ingestStartedAt = System.currentTimeMillis();

        dataset.setStatus("PROCESSING");
        dataset.setStartedAt(Instant.now());
        dataset.setFinishedAt(null);
        datasetRepository.save(dataset);
        clearDatasetCaches(datasetId);
        if (datasetJobService != null) {
            datasetJobService.markProcessing(datasetId);
        }

        try (Reader reader = Files.newBufferedReader(Path.of(dataset.getStoragePath()), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreEmptyLines(true)
                     .setTrim(true)
                     .build()
                     .parse(reader)) {

            List<String> headers = parser.getHeaderNames();
            dataset.setColumnCount(headers.size());

            List<ColumnAccumulator> accumulators = headers.stream()
                    .map(ColumnAccumulator::new)
                    .toList();

            datasetRowRepository.deleteByDatasetId(datasetId);
            datasetColumnProfileRepository.deleteByDatasetId(datasetId);

            List<DatasetRow> rowsToSave = new ArrayList<>(ROW_BATCH_SIZE);
            long rowNumber = 0;

            for (CSVRecord csvRecord : parser) {
                rowNumber++;
                LinkedHashMap<String, Object> rowMap = new LinkedHashMap<>();

                for (int index = 0; index < headers.size(); index++) {
                    String header = headers.get(index);
                    String value = csvRecord.isMapped(header) ? csvRecord.get(header) : "";
                    rowMap.put(header, value);
                    accumulators.get(index).observe(value);
                }

                DatasetRow row = new DatasetRow();
                row.setDatasetId(datasetId);
                row.setRowNumber(rowNumber);
                row.setRowJson(objectMapper.writeValueAsString(rowMap));
                rowsToSave.add(row);

                if (rowsToSave.size() >= ROW_BATCH_SIZE) {
                    datasetRowRepository.saveAll(rowsToSave);
                    rowsToSave.clear();
                    if (datasetJobService != null) {
                        datasetJobService.setTotalRows(datasetId, rowNumber);
                        datasetJobService.updateProgress(datasetId, rowNumber);
                    }
                }
            }

            if (!rowsToSave.isEmpty()) {
                datasetRowRepository.saveAll(rowsToSave);
            }

            List<DatasetColumnProfile> profiles = new ArrayList<>(headers.size());
            for (int index = 0; index < headers.size(); index++) {
                profiles.add(accumulators.get(index).toProfile(datasetId, index));
            }
            datasetColumnProfileRepository.saveAll(profiles);

            dataset.setRowCount(rowNumber);
            dataset.setStatus("READY");
            dataset.setErrorMessage(null);
            dataset.setFinishedAt(Instant.now());
            datasetRepository.save(dataset);
            if (datasetJobService != null) {
                datasetJobService.markCompleted(datasetId, rowNumber);
            }
            long elapsed = System.currentTimeMillis() - ingestStartedAt;
            if (elapsed > slowIngestionWarnThresholdMs) {
                log.warn(
                        "Dataset {} ingested slowly in {} ms with {} rows and {} columns",
                        datasetId,
                        elapsed,
                        rowNumber,
                        headers.size()
                );
            }
            log.info("Dataset {} ingested successfully with {} rows and {} columns", datasetId, rowNumber, headers.size());
        } catch (Exception exception) {
            dataset.setStatus("FAILED");
            dataset.setErrorMessage(exception.getMessage());
            dataset.setFinishedAt(Instant.now());
            datasetRepository.save(dataset);
            if (datasetJobService != null) {
                datasetJobService.markFailed(datasetId, exception.getMessage());
            }
            log.error("Dataset ingestion failed for {}", datasetId, exception);
        }
    }

    private Map<String, Object> readRowJson(String rowJson) {
        try {
            return objectMapper.readValue(rowJson, new TypeReference<>() {});
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read stored row JSON.", exception);
        }
    }

    private boolean matchesAllFilters(
            Map<String, Object> row,
            List<DatasetQueryFilter> filters,
            Map<String, DatasetColumnProfile> profileByColumn) {
        for (DatasetQueryFilter filter : filters) {
            validateColumnExists(profileByColumn, filter.getColumn());
            if (!matchesFilter(row.get(filter.getColumn()), filter, profileByColumn.get(filter.getColumn()))) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesFilter(Object rawValue, DatasetQueryFilter filter, DatasetColumnProfile profile) {
        String operator = filter.getOperator().toLowerCase(Locale.ROOT);
        String left = rawValue == null ? "" : rawValue.toString();
        String right = filter.getValue() == null ? "" : filter.getValue();

        if ("NUMBER".equalsIgnoreCase(profile.getInferredType())) {
            double leftNumber = aggregationService.parseDouble(left);
            double rightNumber = aggregationService.parseDouble(right);
            return switch (operator) {
                case "eq" -> Double.compare(leftNumber, rightNumber) == 0;
                case "neq" -> Double.compare(leftNumber, rightNumber) != 0;
                case "gt" -> leftNumber > rightNumber;
                case "gte" -> leftNumber >= rightNumber;
                case "lt" -> leftNumber < rightNumber;
                case "lte" -> leftNumber <= rightNumber;
                default -> throw new IllegalArgumentException("Unsupported numeric filter operator: " + filter.getOperator());
            };
        }

        return switch (operator) {
            case "eq" -> left.equalsIgnoreCase(right);
            case "neq" -> !left.equalsIgnoreCase(right);
            case "contains" -> left.toLowerCase(Locale.ROOT).contains(right.toLowerCase(Locale.ROOT));
            case "startswith" -> left.toLowerCase(Locale.ROOT).startsWith(right.toLowerCase(Locale.ROOT));
            case "endswith" -> left.toLowerCase(Locale.ROOT).endsWith(right.toLowerCase(Locale.ROOT));
            default -> throw new IllegalArgumentException("Unsupported string filter operator: " + filter.getOperator());
        };
    }

    private List<Map<String, Object>> sortRows(
            List<Map<String, Object>> rows,
            List<DatasetQuerySort> sorts,
            Map<String, DatasetColumnProfile> profileByColumn) {
        if (sorts == null || sorts.isEmpty()) {
            return rows;
        }

        Comparator<Map<String, Object>> comparator = null;
        for (DatasetQuerySort sort : sorts) {
            validateColumnExists(profileByColumn, sort.getColumn());
            Comparator<Map<String, Object>> current = Comparator.comparing(
                    row -> sortKey(row.get(sort.getColumn()), profileByColumn.get(sort.getColumn())),
                    Comparator.nullsLast(Comparator.naturalOrder())
            );
            if ("desc".equalsIgnoreCase(sort.getDirection())) {
                current = current.reversed();
            }
            comparator = comparator == null ? current : comparator.thenComparing(current);
        }

        return rows.stream().sorted(comparator).toList();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Comparable sortKey(Object value, DatasetColumnProfile profile) {
        if (value == null) {
            return null;
        }
        return switch (profile.getInferredType().toUpperCase(Locale.ROOT)) {
            case "NUMBER" -> {
                double parsed = aggregationService.parseDouble(value);
                yield Double.isFinite(parsed) ? parsed : null;
            }
            case "DATE", "DATETIME" -> parseTemporal(value.toString());
            case "BOOLEAN" -> Boolean.parseBoolean(value.toString());
            default -> value.toString().toLowerCase(Locale.ROOT);
        };
    }

    private Instant parseTemporal(String value) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDate.parse(value).atStartOfDay().toInstant(java.time.ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
        }
        return Instant.EPOCH;
    }

    private void validateColumnExists(Map<String, DatasetColumnProfile> profileByColumn, String columnName) {
        if (!profileByColumn.containsKey(columnName)) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }
    }

    private String normalizeChartType(String chartType) {
        if (chartType == null || chartType.isBlank()) {
            return "bar";
        }
        return chartType.toLowerCase(Locale.ROOT);
    }

    private String normalizeAggregation(String aggregation) {
        if (aggregation == null || aggregation.isBlank()) {
            return "count";
        }
        return aggregation.toLowerCase(Locale.ROOT);
    }

    private String requireColumn(String column, String field) {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException(field + " is required.");
        }
        return column;
    }

    private String safeFilename(String filename) {
        if (filename == null || filename.isBlank()) {
            return "dataset.csv";
        }
        return Paths.get(filename).getFileName().toString();
    }

    private Dataset createDatasetFromMultipart(Long ownerUserId, String batchId, MultipartFile file) {
        String filename = safeFilename(file.getOriginalFilename());
        validateCsvFilename(filename);

        String datasetId = UUID.randomUUID().toString();
        Path datasetDirectory = datasetRoot.resolve(datasetId);
        Path stagedFile = datasetDirectory.resolve(filename);

        try {
            Files.createDirectories(datasetDirectory);
            file.transferTo(stagedFile);
            return persistDataset(ownerUserId, batchId, datasetId, filename, stagedFile, file.getContentType());
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to store dataset upload.", exception);
        }
    }

    private List<Dataset> extractDatasetsFromArchive(Long ownerUserId, String batchId, MultipartFile archive) {
        String archiveName = safeFilename(archive.getOriginalFilename());
        if (!archiveName.toLowerCase(Locale.ROOT).endsWith(".zip")) {
            throw new IllegalArgumentException("Archive uploads must be ZIP files.");
        }

        List<Dataset> datasets = new ArrayList<>();
        DatasetArchiveExtractionStats stats = new DatasetArchiveExtractionStats();
        long extractedBytes = 0;
        try (ZipInputStream zipInputStream = new ZipInputStream(archive.getInputStream(), StandardCharsets.UTF_8)) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                stats.setEntriesDiscovered(stats.getEntriesDiscovered() + 1);
                if (entry.isDirectory()) {
                    continue;
                }

                String entryName = sanitizeEntryName(entry.getName());
                if (!entryName.toLowerCase(Locale.ROOT).endsWith(".csv")) {
                    stats.setSkippedNonCsvEntries(stats.getSkippedNonCsvEntries() + 1);
                    zipInputStream.closeEntry();
                    continue;
                }
                if (stats.getCsvFilesAccepted() >= maxArchiveEntries) {
                    throw new IllegalArgumentException(
                            "Archive contains too many CSV files. Max allowed: " + maxArchiveEntries
                    );
                }

                String datasetId = UUID.randomUUID().toString();
                Path datasetDirectory = datasetRoot.resolve(datasetId);
                Path stagedFile = datasetDirectory.resolve(entryName);
                Files.createDirectories(stagedFile.getParent());
                extractedBytes += copyZipEntry(zipInputStream, stagedFile);
                if (extractedBytes > maxArchiveUncompressedBytes) {
                    throw new IllegalArgumentException(
                            "Archive is too large after extraction. Max uncompressed bytes: " + maxArchiveUncompressedBytes
                    );
                }
                datasets.add(persistDataset(ownerUserId, batchId, datasetId, entryName, stagedFile, "text/csv"));
                stats.setCsvFilesAccepted(stats.getCsvFilesAccepted() + 1);
                zipInputStream.closeEntry();
            }
        } catch (IOException exception) {
            log.error("Failed to extract archive {} for batch {}", archiveName, batchId, exception);
            throw new IllegalStateException("Unable to extract ZIP archive.", exception);
        }

        log.info(
                "Archive {} processed for batch {}: {} entries discovered, {} CSV datasets accepted, {} skipped",
                archiveName,
                batchId,
                stats.getEntriesDiscovered(),
                stats.getCsvFilesAccepted(),
                stats.getSkippedNonCsvEntries()
        );

        return datasets;
    }

    private long copyZipEntry(ZipInputStream zipInputStream, Path stagedFile) throws IOException {
        Files.createDirectories(stagedFile.getParent());
        long totalBytes = 0;
        byte[] buffer = new byte[64 * 1024];
        try (var outputStream = Files.newOutputStream(
                stagedFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )) {
            int bytesRead;
            while ((bytesRead = zipInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }
        }
        return totalBytes;
    }

    private Dataset persistDataset(Long ownerUserId, String batchId, String datasetId, String filename, Path stagedFile, String contentType) {
        Dataset dataset = new Dataset();
        dataset.setDatasetId(datasetId);
        dataset.setBatchId(batchId);
        dataset.setOwnerUserId(ownerUserId);
        dataset.setOriginalFilename(filename);
        dataset.setStoredFilename(stagedFile.getFileName().toString());
        dataset.setStoragePath(stagedFile.toString());
        dataset.setContentType(contentType == null ? detectContentType(filename) : contentType);
        dataset.setFileSizeBytes(stagedFile.toFile().length());
        dataset.setStatus("QUEUED");
        dataset.setDatasetType("CSV");
        dataset.setRowCount(0);
        dataset.setColumnCount(0);
        dataset.setCreatedAt(Instant.now());
        return datasetRepository.save(dataset);
    }

    private void validateCsvFilename(String filename) {
        if (!filename.toLowerCase(Locale.ROOT).endsWith(".csv")) {
            throw new IllegalArgumentException("Only CSV files are supported directly. ZIP archives may contain CSV files.");
        }
    }

    private String sanitizeEntryName(String entryName) {
        String normalized = entryName.replace('\\', '/');
        Path safePath = Paths.get(normalized).normalize();
        if (safePath.isAbsolute() || safePath.startsWith("..")) {
            throw new IllegalArgumentException("Archive contains an invalid path: " + entryName);
        }
        return safePath.toString();
    }

    private DatasetChartResponse buildCategoryAggregate(
            String datasetId,
            String chartType,
            String xColumn,
            String rawYColumn,
            String aggregation,
            int limit,
            List<Map<String, Object>> rows) {
        String yColumn = "count".equals(aggregation) ? null : requireColumn(rawYColumn, "yColumn");
        List<Map<String, Object>> aggregates = aggregationService.aggregate(rows, xColumn, yColumn, aggregation, limit);

        DatasetChartResponse response = baseChartResponse(datasetId, chartType, xColumn, yColumn, aggregation);
        response.setLabels(aggregates.stream().map(item -> item.get("group").toString()).toList());
        response.setValues(aggregates.stream().map(item -> ((Number) item.get("value")).doubleValue()).toList());
        return response;
    }

    private DatasetChartResponse buildOrderedAggregate(
            String datasetId,
            String chartType,
            String xColumn,
            String rawYColumn,
            String aggregation,
            int limit,
            List<Map<String, Object>> rows) {
        String yColumn = "count".equals(aggregation) ? null : requireColumn(rawYColumn, "yColumn");
        List<Map<String, Object>> aggregates = aggregationService.aggregate(rows, xColumn, yColumn, aggregation, Integer.MAX_VALUE)
                .stream()
                .sorted((left, right) -> compareNatural(left.get("group").toString(), right.get("group").toString()))
                .limit(limit)
                .toList();

        DatasetChartResponse response = baseChartResponse(datasetId, chartType, xColumn, yColumn, aggregation);
        response.setLabels(aggregates.stream().map(item -> item.get("group").toString()).toList());
        response.setValues(aggregates.stream().map(item -> ((Number) item.get("value")).doubleValue()).toList());
        return response;
    }

    private DatasetChartResponse buildHistogram(
            String datasetId,
            String xColumn,
            String rawYColumn,
            int limit,
            List<Map<String, Object>> rows) {
        String sourceColumn = rawYColumn == null || rawYColumn.isBlank() ? xColumn : rawYColumn;
        List<Map<String, Object>> histogram = aggregationService.histogram(rows, sourceColumn, Math.max(5, Math.min(limit, 20)));
        if (histogram.isEmpty()) {
            throw new IllegalArgumentException("Histogram requires a numeric column.");
        }

        DatasetChartResponse response = baseChartResponse(datasetId, "histogram", sourceColumn, sourceColumn, "frequency");
        response.setLabels(histogram.stream().map(item -> item.get("bucket").toString()).toList());
        response.setValues(histogram.stream().map(item -> ((Number) item.get("count")).doubleValue()).toList());
        return response;
    }

    private DatasetChartResponse buildScatter(
            String datasetId,
            String xColumn,
            String rawYColumn,
            int limit,
            List<Map<String, Object>> rows) {
        String yColumn = requireColumn(rawYColumn, "yColumn");
        List<DatasetChartPoint> points = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            double xValue = aggregationService.parseDouble(row.get(xColumn));
            double yValue = aggregationService.parseDouble(row.get(yColumn));
            if (Double.isFinite(xValue) && Double.isFinite(yValue)) {
                points.add(new DatasetChartPoint(
                        aggregationService.stringify(row.get(xColumn)) + " / " + aggregationService.stringify(row.get(yColumn)),
                        xValue,
                        yValue
                ));
            }
            if (points.size() >= Math.max(50, limit * 20)) {
                break;
            }
        }

        if (points.isEmpty()) {
            throw new IllegalArgumentException("Scatter plot requires two numeric columns.");
        }

        DatasetChartResponse response = baseChartResponse(datasetId, "scatter", xColumn, yColumn, "points");
        response.setPoints(points);
        return response;
    }

    private DatasetChartResponse buildBoxPlot(
            String datasetId,
            String xColumn,
            String rawYColumn,
            List<Map<String, Object>> rows) {
        String sourceColumn = rawYColumn == null || rawYColumn.isBlank() ? xColumn : rawYColumn;
        List<Double> values = rows.stream()
                .map(row -> aggregationService.parseDouble(row.get(sourceColumn)))
                .filter(Double::isFinite)
                .sorted()
                .toList();

        if (values.isEmpty()) {
            throw new IllegalArgumentException("Box plot requires a numeric column.");
        }

        DatasetBoxPlotStats stats = new DatasetBoxPlotStats();
        stats.setMin(values.get(0));
        stats.setQ1(percentile(values, 25));
        stats.setMedian(percentile(values, 50));
        stats.setQ3(percentile(values, 75));
        stats.setMax(values.get(values.size() - 1));

        DatasetChartResponse response = baseChartResponse(datasetId, "box", sourceColumn, sourceColumn, "distribution");
        response.setBoxPlot(stats);
        return response;
    }

    private DatasetChartResponse baseChartResponse(
            String datasetId,
            String chartType,
            String xColumn,
            String yColumn,
            String aggregation) {
        DatasetChartResponse response = new DatasetChartResponse();
        response.setDatasetId(datasetId);
        response.setChartType(chartType);
        response.setXColumn(xColumn);
        response.setYColumn(yColumn);
        response.setAggregation(aggregation);
        return response;
    }

    private double percentile(List<Double> values, int percentile) {
        if (values.isEmpty()) {
            return 0;
        }
        double index = (percentile / 100.0) * (values.size() - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) {
            return values.get(lower);
        }
        double weight = index - lower;
        return values.get(lower) + ((values.get(upper) - values.get(lower)) * weight);
    }

    private int compareNatural(String left, String right) {
        Double leftNumber = tryParseDouble(left);
        Double rightNumber = tryParseDouble(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }
        return left.compareToIgnoreCase(right);
    }

    private Double tryParseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private String detectContentType(String filename) {
        String detected = URLConnection.guessContentTypeFromName(filename);
        return detected == null ? "text/csv" : detected;
    }

    private Dataset requireOwnedDataset(String datasetId, Long ownerUserId) {
        return datasetRepository.findByDatasetIdAndOwnerUserId(datasetId, ownerUserId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found."));
    }

    private int safeLimit(Integer limit) {
        return limit == null ? 25 : Math.max(1, Math.min(limit, 500));
    }

    private int safeOffset(Integer offset) {
        return offset == null ? 0 : Math.max(offset, 0);
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private String buildQueryCacheKey(String datasetId, DatasetQueryRequest request) {
        try {
            return "dataset:query:" + datasetId + ":" + objectMapper.writeValueAsString(request);
        } catch (JsonProcessingException exception) {
            return "dataset:query:" + datasetId + ":" + request.hashCode();
        }
    }

    private String buildChartCacheKey(String datasetId, DatasetChartRequest request) {
        try {
            return "dataset:chart:" + datasetId + ":" + objectMapper.writeValueAsString(request);
        } catch (JsonProcessingException exception) {
            return "dataset:chart:" + datasetId + ":" + request.hashCode();
        }
    }

    private <T> T readCache(String key, Class<T> targetType) {
        if (redisTemplate == null) {
            return null;
        }
        try {
            String payload = redisTemplate.opsForValue().get(key);
            if (payload == null) {
                return null;
            }
            return objectMapper.readValue(payload, targetType);
        } catch (Exception exception) {
            log.debug("Unable to read dataset cache key {}", key, exception);
            return null;
        }
    }

    private void writeCache(String key, Object value) {
        if (redisTemplate == null) {
            return;
        }
        try {
            redisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(value), cacheTtlSeconds, TimeUnit.SECONDS);
        } catch (Exception exception) {
            log.debug("Unable to write dataset cache key {}", key, exception);
        }
    }

    private void clearDatasetCaches(String datasetId) {
        if (redisTemplate == null) {
            return;
        }
        try {
            redisTemplate.delete(redisTemplate.keys("dataset:*:" + datasetId + ":*"));
            redisTemplate.delete(redisTemplate.keys("dataset:query:" + datasetId + ":*"));
            redisTemplate.delete(redisTemplate.keys("dataset:chart:" + datasetId + ":*"));
        } catch (Exception exception) {
            log.debug("Unable to clear dataset cache for {}", datasetId, exception);
        }
    }

    private static class ColumnAccumulator {
        private final String columnName;
        private long nullCount;
        private long nonNullCount;
        private final LinkedHashSet<String> distinctValues = new LinkedHashSet<>();
        private String sampleValue;
        private boolean allNumbers = true;
        private boolean allBooleans = true;
        private boolean allDates = true;
        private boolean allDateTimes = true;
        private Double numericMin;
        private Double numericMax;
        private String lexicalMin;
        private String lexicalMax;

        private ColumnAccumulator(String columnName) {
            this.columnName = columnName;
        }

        private void observe(String rawValue) {
            String value = rawValue == null ? "" : rawValue.trim();
            if (value.isEmpty()) {
                nullCount++;
                return;
            }

            nonNullCount++;
            if (sampleValue == null) {
                sampleValue = value;
            }
            if (distinctValues.size() < 1000) {
                distinctValues.add(value);
            }

            updateLexicalBounds(value);
            updateNumberFlags(value);
            updateBooleanFlags(value);
            updateDateFlags(value);
        }

        private void updateLexicalBounds(String value) {
            if (lexicalMin == null || value.compareTo(lexicalMin) < 0) {
                lexicalMin = value;
            }
            if (lexicalMax == null || value.compareTo(lexicalMax) > 0) {
                lexicalMax = value;
            }
        }

        private void updateNumberFlags(String value) {
            try {
                double parsed = Double.parseDouble(value);
                if (numericMin == null || parsed < numericMin) {
                    numericMin = parsed;
                }
                if (numericMax == null || parsed > numericMax) {
                    numericMax = parsed;
                }
            } catch (NumberFormatException exception) {
                allNumbers = false;
            }
        }

        private void updateBooleanFlags(String value) {
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                allBooleans = false;
            }
        }

        private void updateDateFlags(String value) {
            try {
                LocalDate.parse(value);
            } catch (DateTimeParseException exception) {
                allDates = false;
            }

            try {
                OffsetDateTime.parse(value);
            } catch (DateTimeParseException exception) {
                allDateTimes = false;
            }
        }

        private DatasetColumnProfile toProfile(String datasetId, int index) {
            DatasetColumnProfile profile = new DatasetColumnProfile();
            profile.setDatasetId(datasetId);
            profile.setColumnName(columnName);
            profile.setColumnOrderIndex(index);
            profile.setInferredType(inferType());
            profile.setNonNullCount(nonNullCount);
            profile.setNullCount(nullCount);
            profile.setDistinctCount(distinctValues.size());
            profile.setSampleValue(sampleValue);
            if ("NUMBER".equals(profile.getInferredType())) {
                profile.setMinValue(numericMin == null ? null : String.valueOf(numericMin));
                profile.setMaxValue(numericMax == null ? null : String.valueOf(numericMax));
            } else {
                profile.setMinValue(lexicalMin);
                profile.setMaxValue(lexicalMax);
            }
            return profile;
        }

        private String inferType() {
            if (nonNullCount == 0) {
                return "STRING";
            }
            if (allNumbers) {
                return "NUMBER";
            }
            if (allBooleans) {
                return "BOOLEAN";
            }
            if (allDateTimes) {
                return "DATETIME";
            }
            if (allDates) {
                return "DATE";
            }
            return "STRING";
        }
    }
}
