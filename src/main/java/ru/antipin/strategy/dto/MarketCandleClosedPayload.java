package ru.antipin.strategy.dto;

public record MarketCandleClosedPayload(
        String runId,
        String groupId,
        InstrumentPayload instrument,
        String timeframe,
        Long sequenceNumber,
        String candleTime,
        String open,
        String high,
        String low,
        String close,
        Long volume,
        Boolean isWarmup
) {
}
