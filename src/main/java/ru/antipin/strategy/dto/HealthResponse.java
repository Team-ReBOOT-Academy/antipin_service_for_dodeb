package ru.antipin.strategy.dto;

import java.util.Map;
public record HealthResponse(
        String status,
        String service,
        String version,
        Map<String, ComponentStatus> components
) {
}