package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.service.DatasetService;
import com.abhishek.scalable_backend_system.service.RequestAuthService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/saved-charts")
public class SavedChartController {

    private final DatasetService datasetService;
    private final RequestAuthService requestAuthService;

    public SavedChartController(DatasetService datasetService, RequestAuthService requestAuthService) {
        this.datasetService = datasetService;
        this.requestAuthService = requestAuthService;
    }

    @DeleteMapping("/{chartId}")
    public ResponseEntity<Void> delete(@PathVariable String chartId, HttpServletRequest request) {
        datasetService.deleteSavedChart(chartId, requestAuthService.requireCurrentUserId(request));
        return ResponseEntity.noContent().build();
    }
}
