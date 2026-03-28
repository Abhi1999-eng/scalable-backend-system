package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.Dataset;
import com.abhishek.scalable_backend_system.model.SavedChart;
import com.abhishek.scalable_backend_system.model.SavedChartRequest;
import com.abhishek.scalable_backend_system.repository.SavedChartRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class SavedChartService {

    private final SavedChartRepository savedChartRepository;

    public SavedChartService(SavedChartRepository savedChartRepository) {
        this.savedChartRepository = savedChartRepository;
    }

    public List<SavedChart> listForDataset(String datasetId, Long ownerUserId) {
        return savedChartRepository.findByDatasetIdAndOwnerUserIdOrderByUpdatedAtDesc(datasetId, ownerUserId);
    }

    public SavedChart save(Dataset dataset, Long ownerUserId, SavedChartRequest request) {
        SavedChart chart = new SavedChart();
        chart.setChartId(UUID.randomUUID().toString());
        chart.setOwnerUserId(ownerUserId);
        chart.setDatasetId(dataset.getDatasetId());
        chart.setName(request.getName().trim());
        chart.setChartType(request.getChartType().trim().toLowerCase());
        chart.setXColumn(request.getXColumn().trim());
        chart.setYColumn(request.getYColumn() == null || request.getYColumn().isBlank() ? null : request.getYColumn().trim());
        chart.setAggregation(request.getAggregation().trim().toLowerCase());
        chart.setLimitValue(request.getLimit());
        chart.setCreatedAt(Instant.now());
        chart.setUpdatedAt(Instant.now());
        return savedChartRepository.save(chart);
    }

    public void delete(String chartId, Long ownerUserId) {
        SavedChart chart = savedChartRepository.findByChartIdAndOwnerUserId(chartId, ownerUserId)
                .orElseThrow(() -> new IllegalArgumentException("Saved chart not found."));
        savedChartRepository.delete(chart);
    }
}
