package ru.antipin.strategy.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.antipin.strategy.dto.InstrumentPayload;
import ru.antipin.strategy.dto.MarketCandleClosedPayload;
import ru.antipin.strategy.dto.RecommendationCreatedPayload;
import ru.antipin.strategy.event.EventEnvelope;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Service
public class RecommendationService {
    private final String secretKey;
    private final String accountId;
    private final String groupId;
    private final String strategyId;
    private final String serviceName;

    public RecommendationService(
            @Value("${app.strategy.secret-key}") String secretKey,
            @Value("${app.strategy.account-id}") String accountId,
            @Value("${app.strategy.group-id}") String groupId,
            @Value("${app.strategy.strategy-id}") String strategyId,
            @Value("${app.service-name}") String serviceName
        ){
        this.secretKey = secretKey;
        this.accountId = accountId;
        this.groupId = groupId;
        this.strategyId = strategyId;
        this.serviceName = serviceName;
    }
    public EventEnvelope<RecommendationCreatedPayload> processCandle(
            EventEnvelope<MarketCandleClosedPayload> envelope
    ) {
        return processCandle(envelope, System.nanoTime());
    }
    public EventEnvelope<RecommendationCreatedPayload> processCandle(
            EventEnvelope<MarketCandleClosedPayload> envelope,
            long startNanos
    ) {
        validateCandleEnvelope(envelope);
        MarketCandleClosedPayload candle = envelope.payload();
        long processingTimeMs = Math.max(0L, (System.nanoTime() - startNanos) / 1_000_000L);
        RecommendationCreatedPayload payload = new RecommendationCreatedPayload(
                UUID.randomUUID().toString(),
                envelope.eventId(),
                candle.runId(),
                secretKey,
                strategyId,
                accountId,
                groupId,
                candle.instrument().ticker(),
                candle.sequenceNumber(),
                "HOLD",
                0.0,
                0,
                "OK",
                "Baseline HOLD recommendation (Practice 3)",
                processingTimeMs,
                Map.of("mode", "baseline-hold", "timeframe", candle.timeframe())
        );
        return EventEnvelope.create(
                "strategy.recommendation.created",
                envelope.correlationId(),
                envelope.eventId(),
                serviceName,
                payload
        );
    }
    private void validateCandleEnvelope(EventEnvelope<MarketCandleClosedPayload> envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("Общая обертка события отсутствует");
        }
        requireNonBlank(envelope.eventId(), "eventId");
        if (!"market.candle.closed".equals(envelope.eventType())) {
            throw new IllegalArgumentException("Неожиданный eventType: " + envelope.eventType());
        }
        if (!"1.0".equals(envelope.schemaVersion())) {
            throw new IllegalArgumentException("Неподдерживаемая schemaVersion: " + envelope.schemaVersion());
        }
        requireNonBlank(envelope.occurredAt(), "occurredAt");
        requireNonBlank(envelope.correlationId(), "correlationId");
        requireNonBlank(envelope.producer(), "producer");
        MarketCandleClosedPayload candle = envelope.payload();
        if (candle == null) {
            throw new IllegalArgumentException("payload свечи отсутствует");
        }
        requireNonBlank(candle.runId(), "payload.runId");
        requireNonBlank(candle.groupId(), "payload.groupId");
        InstrumentPayload instrument = candle.instrument();
        if (instrument == null) {
            throw new IllegalArgumentException("payload.instrument отсутствует");
        }
        requireNonBlank(instrument.ticker(), "payload.instrument.ticker");
        requireNonBlank(instrument.currency(), "payload.instrument.currency");
        if (instrument.lotSize() == null || instrument.lotSize() <= 0) {
            throw new IllegalArgumentException("payload.instrument.lotSize должен быть > 0");
        }
        requireNonBlank(candle.timeframe(), "payload.timeframe");
        if (candle.sequenceNumber() == null || candle.sequenceNumber() < 1) {
            throw new IllegalArgumentException("payload.sequenceNumber должен быть >= 1");
        }
        requireNonBlank(candle.candleTime(), "payload.candleTime");
        requirePositivePrice(candle.open(), "payload.open");
        requirePositivePrice(candle.high(), "payload.high");
        requirePositivePrice(candle.low(), "payload.low");
        requirePositivePrice(candle.close(), "payload.close");
        if (candle.volume() == null || candle.volume() < 0) {
            throw new IllegalArgumentException("payload.volume должен быть >= 0");
        }
        if (candle.isWarmup() == null) {
            throw new IllegalArgumentException("payload.isWarmup отсутствует");
        }
    }
    private void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Отсутствует обязательное поле: " + fieldName);
        }
    }
    private void requirePositivePrice(String value, String fieldName) {
        requireNonBlank(value, fieldName);
        try {
            if (new BigDecimal(value).compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalArgumentException("Цена должна быть > 0: " + fieldName);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Некорректный формат числа в поле: " + fieldName);
        }
    }
}
