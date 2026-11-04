package ru.antipin.strategy.event;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(
        String eventId,
        String eventType,
        String schemaVersion,
        String occurredAt,
        String correlationId,
        String causationId,
        String producer,
        T payload
) {
    public static <T> EventEnvelope<T> create(
            String eventType,
            String correlationId,
            String causationId,
            String producer,
            T payload
    ){
        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                eventType,
                "1.0",
                Instant.now().toString(),
                correlationId,
                causationId,
                producer,
                payload
        );
    }

}
