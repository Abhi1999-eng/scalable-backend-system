package com.abhishek.scalable_backend_system.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class DatasetAggregationService {

    public List<Map<String, Object>> aggregate(
            List<Map<String, Object>> rows,
            String groupByColumn,
            String aggregateColumn,
            String aggregation,
            int limit) {
        String normalized = aggregation == null ? "count" : aggregation.toLowerCase(Locale.ROOT);
        Map<String, double[]> buckets = new LinkedHashMap<>();

        for (Map<String, Object> row : rows) {
            String key = stringify(row.get(groupByColumn));
            double[] state = buckets.computeIfAbsent(key, ignored -> new double[] {0, 0});
            state[0] += 1;
            if (!"count".equals(normalized) && aggregateColumn != null && !aggregateColumn.isBlank()) {
                double numeric = parseDouble(row.get(aggregateColumn));
                if (Double.isFinite(numeric)) {
                    state[1] += numeric;
                }
            }
        }

        return buckets.entrySet()
                .stream()
                .map(entry -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("group", entry.getKey());
                    result.put("count", entry.getValue()[0]);
                    result.put("value", switch (normalized) {
                        case "sum" -> entry.getValue()[1];
                        case "avg" -> entry.getValue()[0] == 0 ? 0 : entry.getValue()[1] / entry.getValue()[0];
                        default -> entry.getValue()[0];
                    });
                    return result;
                })
                .sorted(Comparator.comparingDouble(item -> -((Number) item.get("value")).doubleValue()))
                .limit(limit)
                .toList();
    }

    public List<Map<String, Object>> histogram(
            List<Map<String, Object>> rows,
            String sourceColumn,
            int bins) {
        List<Double> values = rows.stream()
                .map(row -> parseDouble(row.get(sourceColumn)))
                .filter(Double::isFinite)
                .sorted()
                .toList();

        if (values.isEmpty()) {
            return List.of();
        }

        int safeBins = Math.max(1, bins);
        double min = values.get(0);
        double max = values.get(values.size() - 1);
        double range = max - min;
        double binSize = range == 0 ? 1 : range / safeBins;
        double[] counts = new double[safeBins];

        for (double value : values) {
            int index = range == 0 ? 0 : Math.min((int) ((value - min) / binSize), safeBins - 1);
            counts[index] += 1;
        }

        List<Map<String, Object>> result = new ArrayList<>(safeBins);
        for (int index = 0; index < safeBins; index++) {
            double start = min + (index * binSize);
            double end = start + binSize;
            Map<String, Object> bucket = new LinkedHashMap<>();
            bucket.put("bucket", String.format(Locale.ROOT, "%.2f - %.2f", start, end));
            bucket.put("count", counts[index]);
            result.add(bucket);
        }
        return result;
    }

    public double parseDouble(Object value) {
        if (value == null) {
            return Double.NaN;
        }
        try {
            double parsed = Double.parseDouble(value.toString());
            return Double.isFinite(parsed) ? parsed : Double.NaN;
        } catch (NumberFormatException exception) {
            return Double.NaN;
        }
    }

    public String stringify(Object value) {
        if (value == null || value.toString().isBlank()) {
            return "(blank)";
        }
        return value.toString();
    }
}
