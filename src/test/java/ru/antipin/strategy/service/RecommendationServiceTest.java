package ru.antipin.strategy.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.antipin.strategy.dto.InstrumentPayload;
import ru.antipin.strategy.dto.MarketCandleClosedPayload;
import ru.antipin.strategy.dto.RecommendationCreatedPayload;
import ru.antipin.strategy.event.EventEnvelope;

import java.io.InputStream;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RecommendationServiceTest {

    private ObjectMapper objectMapper;
    private RecommendationService recommendationService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        recommendationService = new RecommendationService(
                "test-secret-key",
                "antipin-aa",
                "МПБ-401",
                "rsi-reversal",
                "antipin_service_for_dodeb"
        );
    }

    @Test
    void shouldConvertSampleCandleEventToHoldRecommendation() throws Exception {
        EventEnvelope<MarketCandleClosedPayload> candleEnvelope;
        try (InputStream is = getClass().getResourceAsStream("/sample-candle-closed-event.json")) {
            assertNotNull(is, "Тестовый файл /sample-candle-closed-event.json должен существовать");
            candleEnvelope = objectMapper.readValue(is, new TypeReference<>() {});
        }

        EventEnvelope<RecommendationCreatedPayload> result = recommendationService.processCandle(candleEnvelope);

        assertNotNull(result);
        assertDoesNotThrow(() -> UUID.fromString(result.eventId()));
        assertNotEquals(candleEnvelope.eventId(), result.eventId());
        assertEquals("strategy.recommendation.created", result.eventType());
        assertEquals("1.0", result.schemaVersion());
        assertDoesNotThrow(() -> Instant.parse(result.occurredAt()));
        assertEquals("run-42-SBER-000051", result.correlationId());
        assertEquals("8b72c40f-1172-4fb4-9878-174c9bc72264", result.causationId());
        assertEquals("antipin_service_for_dodeb", result.producer());

        RecommendationCreatedPayload payload = result.payload();
        assertNotNull(payload);
        assertDoesNotThrow(() -> UUID.fromString(payload.recommendationId()));
        assertEquals("8b72c40f-1172-4fb4-9878-174c9bc72264", payload.sourceEventId());
        assertEquals("run-42", payload.runId());
        assertEquals("test-secret-key", payload.secretKey());
        assertEquals("rsi-reversal", payload.strategyId());
        assertEquals("antipin-aa", payload.accountId());
        assertEquals("МПБ-401", payload.groupId());
        assertEquals("SBER", payload.instrument());
        assertEquals(51L, payload.sequenceNumber());
        assertEquals("HOLD", payload.signal());
        assertEquals(0.0, payload.confidence());
        assertEquals(0, payload.suggestedQuantity());
        assertEquals("OK", payload.dataQuality());
        assertNotNull(payload.reason());
        assertFalse(payload.reason().isBlank());
        assertNotNull(payload.processingTimeMs());
        assertTrue(payload.processingTimeMs() >= 0L);
        assertNotNull(payload.metadata());
        assertFalse(payload.metadata().isEmpty());
    }

    @Test
    void shouldRejectInvalidCandleMessages() {
        MarketCandleClosedPayload validPayload = new MarketCandleClosedPayload(
                "run-42",
                "market",
                new InstrumentPayload("SBER", "", "RUB", 10),
                "1m",
                51L,
                "2026-09-24T08:15:00Z",
                "281.10",
                "281.80",
                "280.90",
                "281.40",
                150000L,
                false
        );

        EventEnvelope<MarketCandleClosedPayload> wrongType = new EventEnvelope<>(
                "evt-1", "account.service.heartbeat", "1.0",
                "2026-09-24T08:15:01Z", "corr-1", null, "loader", validPayload
        );
        assertThrows(IllegalArgumentException.class, () -> recommendationService.processCandle(wrongType));

        MarketCandleClosedPayload missingRunId = new MarketCandleClosedPayload(
                " ", "market", validPayload.instrument(), "1m", 1L,
                "2026-09-24T08:15:00Z", "281.10", "281.80", "280.90", "281.40", 100L, false
        );
        EventEnvelope<MarketCandleClosedPayload> badRunId = EventEnvelope.create(
                "market.candle.closed", "corr-1", null, "loader", missingRunId
        );
        assertThrows(IllegalArgumentException.class, () -> recommendationService.processCandle(badRunId));

        MarketCandleClosedPayload badPricePayload = new MarketCandleClosedPayload(
                "run-42", "market", validPayload.instrument(), "1m", 1L,
                "2026-09-24T08:15:00Z", "281.10", "281.80", "280.90", "invalid-price", 100L, false
        );
        EventEnvelope<MarketCandleClosedPayload> badPrice = EventEnvelope.create(
                "market.candle.closed", "corr-1", null, "loader", badPricePayload
        );
        assertThrows(IllegalArgumentException.class, () -> recommendationService.processCandle(badPrice));

        MarketCandleClosedPayload badSequencePayload = new MarketCandleClosedPayload(
                "run-42", "market", validPayload.instrument(), "1m", 0L,
                "2026-09-24T08:15:00Z", "281.10", "281.80", "280.90", "281.40", 100L, false
        );
        EventEnvelope<MarketCandleClosedPayload> badSeq = EventEnvelope.create(
                "market.candle.closed", "corr-1", null, "loader", badSequencePayload
        );
        assertThrows(IllegalArgumentException.class, () -> recommendationService.processCandle(badSeq));
    }
}
