package ru.antipin.strategy.dto;

public record InstrumentPayload(
        String ticker,
        String figi,
        String currency,
        Integer lotSize
        ) {
}
