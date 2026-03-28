package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.Dataset;
import com.abhishek.scalable_backend_system.model.DatasetChartRequest;
import com.abhishek.scalable_backend_system.model.DatasetChartResponse;
import com.abhishek.scalable_backend_system.model.DatasetColumnProfile;
import com.abhishek.scalable_backend_system.model.DatasetRow;
import com.abhishek.scalable_backend_system.repository.DatasetColumnProfileRepository;
import com.abhishek.scalable_backend_system.repository.DatasetRepository;
import com.abhishek.scalable_backend_system.repository.DatasetRowRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.mock.web.MockMultipartFile;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DatasetServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldIngestCsvDatasetAndBuildProfileAndRows() {
        InMemoryDatasetRepository datasetRepository = new InMemoryDatasetRepository();
        InMemoryDatasetColumnProfileRepository profileRepository = new InMemoryDatasetColumnProfileRepository();
        InMemoryDatasetRowRepository rowRepository = new InMemoryDatasetRowRepository();
        Executor directExecutor = Runnable::run;

        DatasetService service = new DatasetService(
                datasetRepository,
                profileRepository,
                rowRepository,
                new ObjectMapper(),
                directExecutor,
                tempDir.toString()
        );

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "sales.csv",
                "text/csv",
                """
                month,region,revenue
                Jan,North,120
                Jan,South,80
                Feb,North,150
                """.getBytes(StandardCharsets.UTF_8)
        );

        String datasetId = service.submitDataset(file);
        Dataset dataset = service.getDataset(datasetId).orElseThrow();
        List<DatasetColumnProfile> schema = service.getSchema(datasetId);
        List<Map<String, Object>> rows = service.getRows(datasetId, 10, 0);

        assertEquals("READY", dataset.getStatus());
        assertEquals(3, dataset.getRowCount());
        assertEquals(3, dataset.getColumnCount());
        assertEquals(3, schema.size());
        assertEquals("NUMBER", schema.get(2).getInferredType());
        assertEquals(3, rows.size());
        assertEquals("Jan", rows.get(0).get("month"));
    }

    @Test
    void shouldBuildCountChartFromDatasetRows() {
        InMemoryDatasetRepository datasetRepository = new InMemoryDatasetRepository();
        InMemoryDatasetColumnProfileRepository profileRepository = new InMemoryDatasetColumnProfileRepository();
        InMemoryDatasetRowRepository rowRepository = new InMemoryDatasetRowRepository();
        Executor directExecutor = Runnable::run;

        DatasetService service = new DatasetService(
                datasetRepository,
                profileRepository,
                rowRepository,
                new ObjectMapper(),
                directExecutor,
                tempDir.toString()
        );

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "visits.csv",
                "text/csv",
                """
                channel,revenue
                Organic,120
                Organic,60
                Ads,90
                """.getBytes(StandardCharsets.UTF_8)
        );

        String datasetId = service.submitDataset(file);

        DatasetChartRequest request = new DatasetChartRequest();
        request.setChartType("bar");
        request.setXColumn("channel");
        request.setAggregation("count");
        request.setLimit(5);

        DatasetChartResponse response = service.buildChart(datasetId, request);

        assertEquals(List.of("Organic", "Ads"), response.getLabels());
        assertEquals(List.of(2.0, 1.0), response.getValues());
    }

    @Test
    void shouldBuildSumChartFromDatasetRows() {
        InMemoryDatasetRepository datasetRepository = new InMemoryDatasetRepository();
        InMemoryDatasetColumnProfileRepository profileRepository = new InMemoryDatasetColumnProfileRepository();
        InMemoryDatasetRowRepository rowRepository = new InMemoryDatasetRowRepository();
        Executor directExecutor = Runnable::run;

        DatasetService service = new DatasetService(
                datasetRepository,
                profileRepository,
                rowRepository,
                new ObjectMapper(),
                directExecutor,
                tempDir.toString()
        );

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "totals.csv",
                "text/csv",
                """
                week,revenue
                1,120
                1,80
                2,90
                """.getBytes(StandardCharsets.UTF_8)
        );

        String datasetId = service.submitDataset(file);

        DatasetChartRequest request = new DatasetChartRequest();
        request.setChartType("bar");
        request.setXColumn("week");
        request.setYColumn("revenue");
        request.setAggregation("sum");
        request.setLimit(5);

        DatasetChartResponse response = service.buildChart(datasetId, request);

        assertEquals(List.of("1", "2"), response.getLabels());
        assertEquals(List.of(200.0, 90.0), response.getValues());
    }

    @Test
    void shouldRejectNonCsvUploads() {
        DatasetService service = new DatasetService(
                new InMemoryDatasetRepository(),
                new InMemoryDatasetColumnProfileRepository(),
                new InMemoryDatasetRowRepository(),
                new ObjectMapper(),
                Runnable::run,
                tempDir.toString()
        );

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "report.json",
                "application/json",
                "{}".getBytes(StandardCharsets.UTF_8)
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.submitDataset(file)
        );

        assertEquals("Only CSV files are supported directly. ZIP archives may contain CSV files.", exception.getMessage());
    }

    private static class InMemoryDatasetRepository implements DatasetRepository {
        private final List<Dataset> datasets = new ArrayList<>();
        private long sequence = 1;

        @Override
        public Optional<Dataset> findByDatasetId(String datasetId) {
            return datasets.stream().filter(dataset -> datasetId.equals(dataset.getDatasetId())).findFirst();
        }

        @Override
        public List<Dataset> findByBatchIdOrderByCreatedAtAsc(String batchId) {
            return datasets.stream()
                    .filter(dataset -> batchId.equals(dataset.getBatchId()))
                    .toList();
        }

        @Override
        public Optional<Dataset> findByDatasetIdAndOwnerUserId(String datasetId, Long ownerUserId) {
            return datasets.stream()
                    .filter(dataset -> datasetId.equals(dataset.getDatasetId()))
                    .filter(dataset -> ownerUserId == null || ownerUserId.equals(dataset.getOwnerUserId()))
                    .findFirst();
        }

        @Override
        public List<Dataset> findByBatchIdAndOwnerUserIdOrderByCreatedAtAsc(String batchId, Long ownerUserId) {
            return datasets.stream()
                    .filter(dataset -> batchId.equals(dataset.getBatchId()))
                    .filter(dataset -> ownerUserId == null || ownerUserId.equals(dataset.getOwnerUserId()))
                    .toList();
        }

        @Override
        public Page<Dataset> findByOwnerUserIdOrderByCreatedAtDesc(Long ownerUserId, Pageable pageable) {
            List<Dataset> filtered = datasets.stream()
                    .filter(dataset -> ownerUserId == null || ownerUserId.equals(dataset.getOwnerUserId()))
                    .sorted(Comparator.comparing(Dataset::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                    .toList();
            return new PageImpl<>(filtered, pageable, filtered.size());
        }

        @Override
        public <S extends Dataset> S save(S entity) {
            if (entity.getId() == null) {
                entity.setId(sequence++);
                datasets.add(entity);
            }
            return entity;
        }

        @Override
        public List<Dataset> findAll() {
            return datasets;
        }

        @Override public List<Dataset> findAllById(Iterable<Long> longs) { throw new UnsupportedOperationException(); }
        @Override public long count() { return datasets.size(); }
        @Override public void deleteById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public void delete(Dataset entity) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllById(Iterable<? extends Long> longs) { throw new UnsupportedOperationException(); }
        @Override public void deleteAll(Iterable<? extends Dataset> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAll() { datasets.clear(); }
        @Override public <S extends Dataset> List<S> saveAll(Iterable<S> entities) { throw new UnsupportedOperationException(); }
        @Override public Optional<Dataset> findById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public boolean existsById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public void flush() { }
        @Override public <S extends Dataset> S saveAndFlush(S entity) { return save(entity); }
        @Override public <S extends Dataset> List<S> saveAllAndFlush(Iterable<S> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllInBatch(Iterable<Dataset> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllByIdInBatch(Iterable<Long> longs) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllInBatch() { datasets.clear(); }
        @Override public Dataset getOne(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public Dataset getById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public Dataset getReferenceById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset> Optional<S> findOne(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset> List<S> findAll(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset> List<S> findAll(Example<S> example, Sort sort) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset> Page<S> findAll(Example<S> example, Pageable pageable) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset> long count(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset> boolean exists(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends Dataset, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
        @Override public List<Dataset> findAll(Sort sort) { return datasets; }
        @Override public Page<Dataset> findAll(Pageable pageable) { throw new UnsupportedOperationException(); }
    }

    private static class InMemoryDatasetColumnProfileRepository implements DatasetColumnProfileRepository {
        private final List<DatasetColumnProfile> profiles = new ArrayList<>();
        private long sequence = 1;

        @Override
        public List<DatasetColumnProfile> findByDatasetIdOrderByColumnOrderIndexAsc(String datasetId) {
            return profiles.stream()
                    .filter(profile -> datasetId.equals(profile.getDatasetId()))
                    .sorted(Comparator.comparingInt(DatasetColumnProfile::getColumnOrderIndex))
                    .toList();
        }

        @Override
        public void deleteByDatasetId(String datasetId) {
            profiles.removeIf(profile -> datasetId.equals(profile.getDatasetId()));
        }

        @Override
        public <S extends DatasetColumnProfile> List<S> saveAll(Iterable<S> entities) {
            List<S> saved = new ArrayList<>();
            for (S entity : entities) {
                if (entity.getId() == null) {
                    entity.setId(sequence++);
                }
                profiles.add(entity);
                saved.add(entity);
            }
            return saved;
        }

        @Override public <S extends DatasetColumnProfile> S save(S entity) { throw new UnsupportedOperationException(); }
        @Override public Optional<DatasetColumnProfile> findById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public boolean existsById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public List<DatasetColumnProfile> findAll() { return profiles; }
        @Override public List<DatasetColumnProfile> findAllById(Iterable<Long> longs) { throw new UnsupportedOperationException(); }
        @Override public long count() { return profiles.size(); }
        @Override public void deleteById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public void delete(DatasetColumnProfile entity) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllById(Iterable<? extends Long> longs) { throw new UnsupportedOperationException(); }
        @Override public void deleteAll(Iterable<? extends DatasetColumnProfile> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAll() { profiles.clear(); }
        @Override public void flush() { }
        @Override public <S extends DatasetColumnProfile> S saveAndFlush(S entity) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> List<S> saveAllAndFlush(Iterable<S> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllInBatch(Iterable<DatasetColumnProfile> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllByIdInBatch(Iterable<Long> longs) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllInBatch() { profiles.clear(); }
        @Override public DatasetColumnProfile getOne(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public DatasetColumnProfile getById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public DatasetColumnProfile getReferenceById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> Optional<S> findOne(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> List<S> findAll(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> List<S> findAll(Example<S> example, Sort sort) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> Page<S> findAll(Example<S> example, Pageable pageable) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> long count(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile> boolean exists(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetColumnProfile, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
        @Override public List<DatasetColumnProfile> findAll(Sort sort) { return profiles; }
        @Override public Page<DatasetColumnProfile> findAll(Pageable pageable) { throw new UnsupportedOperationException(); }
    }

    private static class InMemoryDatasetRowRepository implements DatasetRowRepository {
        private final List<DatasetRow> rows = new ArrayList<>();
        private long sequence = 1;

        @Override
        public List<DatasetRow> findByDatasetIdOrderByRowNumberAsc(String datasetId, Pageable pageable) {
            List<DatasetRow> filtered = rows.stream()
                    .filter(row -> datasetId.equals(row.getDatasetId()))
                    .sorted(Comparator.comparingLong(DatasetRow::getRowNumber))
                    .toList();
            int start = (int) pageable.getOffset();
            int end = Math.min(filtered.size(), start + pageable.getPageSize());
            if (start >= filtered.size()) {
                return List.of();
            }
            return filtered.subList(start, end);
        }

        @Override
        public List<DatasetRow> findByDatasetId(String datasetId) {
            return rows.stream()
                    .filter(row -> datasetId.equals(row.getDatasetId()))
                    .sorted(Comparator.comparingLong(DatasetRow::getRowNumber))
                    .toList();
        }

        @Override
        public void deleteByDatasetId(String datasetId) {
            rows.removeIf(row -> datasetId.equals(row.getDatasetId()));
        }

        @Override
        public <S extends DatasetRow> List<S> saveAll(Iterable<S> entities) {
            List<S> saved = new ArrayList<>();
            for (S entity : entities) {
                if (entity.getId() == null) {
                    entity.setId(sequence++);
                }
                rows.add(entity);
                saved.add(entity);
            }
            return saved;
        }

        @Override public <S extends DatasetRow> S save(S entity) { throw new UnsupportedOperationException(); }
        @Override public Optional<DatasetRow> findById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public boolean existsById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public List<DatasetRow> findAll() { return rows; }
        @Override public List<DatasetRow> findAllById(Iterable<Long> longs) { throw new UnsupportedOperationException(); }
        @Override public long count() { return rows.size(); }
        @Override public void deleteById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public void delete(DatasetRow entity) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllById(Iterable<? extends Long> longs) { throw new UnsupportedOperationException(); }
        @Override public void deleteAll(Iterable<? extends DatasetRow> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAll() { rows.clear(); }
        @Override public void flush() { }
        @Override public <S extends DatasetRow> S saveAndFlush(S entity) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> List<S> saveAllAndFlush(Iterable<S> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllInBatch(Iterable<DatasetRow> entities) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllByIdInBatch(Iterable<Long> longs) { throw new UnsupportedOperationException(); }
        @Override public void deleteAllInBatch() { rows.clear(); }
        @Override public DatasetRow getOne(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public DatasetRow getById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public DatasetRow getReferenceById(Long aLong) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> Optional<S> findOne(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> List<S> findAll(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> List<S> findAll(Example<S> example, Sort sort) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> Page<S> findAll(Example<S> example, Pageable pageable) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> long count(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow> boolean exists(Example<S> example) { throw new UnsupportedOperationException(); }
        @Override public <S extends DatasetRow, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
        @Override public List<DatasetRow> findAll(Sort sort) { return rows; }
        @Override public Page<DatasetRow> findAll(Pageable pageable) { throw new UnsupportedOperationException(); }
    }
}
