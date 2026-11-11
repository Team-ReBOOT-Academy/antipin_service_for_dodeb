package ru.antipin.strategy.dto;

import java.util.Map;

public record RecommendationCreatedPayload(
        String recommendationId,
        String sourceEventId,
        String runId,
        String secretKey,
        String strategyId,
        String accountId,
        String groupId,
        String instrument,
        Long sequenceNumber,
        String signal,
        Double confidence,
        Integer suggestedQuantity,
        String dataQuality,
        String reason,
        Long processingTimeMs,
        Map<String, Object> metadata
) {
}
