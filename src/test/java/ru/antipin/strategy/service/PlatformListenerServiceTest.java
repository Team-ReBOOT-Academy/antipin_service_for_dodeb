package ru.antipin.strategy.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import ru.antipin.strategy.dto.RecommendationCreatedPayload;
import ru.antipin.strategy.event.EventEnvelope;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PlatformListenerServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private PlatformListenerService listenerService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        RecommendationService recommendationService = new RecommendationService(
                "test-secret-key",
                "antipin-aa",
                "МПБ-401",
                "rsi-reversal",
                "antipin_service_for_dodeb"
        );

        listenerService = new PlatformListenerService(
                objectMapper,
                recommendationService,
                kafkaTemplate,
                "antipin-aa",
                "strategy.recommendation.created"
        );
    }

    @Test
    void shouldPublishHoldRecommendationForValidCandleSample() throws Exception {
        String registrationOkJson = """
                {
                  "eventId": "reg-res-1",
                  "eventType": "account.registration.result",
                  "schemaVersion": "1.0",
                  "occurredAt": "2026-09-24T08:00:01Z",
                  "correlationId": "reg-antipin-aa",
                  "causationId": "reg-req-1",
                  "producer": "registration-service",
                  "payload": {
                    "secretKey": "test-secret-key",
                    "accountId": "antipin-aa",
                    "groupId": "МПБ-401",
                    "strategyId": "rsi-reversal",
                    "serviceName": "antipin_service_for_dodeb",
                    "status": "REGISTERED",
                    "reason": "Account service has been registered."
                  }
                }
                """;
        listenerService.onRegistrationResult(registrationOkJson);
        assertTrue(listenerService.isRegistered());

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        String sampleCandleJson;
        try (InputStream is = getClass().getResourceAsStream("/sample-candle-closed-event.json")) {
            assertNotNull(is);
            sampleCandleJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertDoesNotThrow(() -> listenerService.onCandleClosed(sampleCandleJson));

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1))
                .send(eq("strategy.recommendation.created"), eq("antipin-aa"), jsonCaptor.capture());

        EventEnvelope<RecommendationCreatedPayload> sentEnvelope = objectMapper.readValue(
                jsonCaptor.getValue(),
                new TypeReference<>() {}
        );
        assertEquals("strategy.recommendation.created", sentEnvelope.eventType());
        assertEquals("run-42-SBER-000051", sentEnvelope.correlationId());
        assertEquals("8b72c40f-1172-4fb4-9878-174c9bc72264", sentEnvelope.causationId());
        assertEquals("antipin_service_for_dodeb", sentEnvelope.producer());

        RecommendationCreatedPayload payload = sentEnvelope.payload();
        assertEquals("8b72c40f-1172-4fb4-9878-174c9bc72264", payload.sourceEventId());
        assertEquals("run-42", payload.runId());
        assertEquals("SBER", payload.instrument());
        assertEquals(51L, payload.sequenceNumber());
        assertEquals("HOLD", payload.signal());
        assertEquals(0.0, payload.confidence());
        assertEquals(0, payload.suggestedQuantity());
    }

    @Test
    void shouldNotCrashOrPublishOnMalformedOrInvalidCandle() {
        assertDoesNotThrow(() -> listenerService.onCandleClosed("{broken json"));
        assertDoesNotThrow(() -> listenerService.onCandleClosed("""
                {
                  "eventId": "bad-candle-1",
                  "eventType": "market.candle.closed",
                  "schemaVersion": "1.0",
                  "occurredAt": "2026-09-24T08:15:01Z",
                  "correlationId": "corr-bad",
                  "producer": "loader",
                  "payload": {
                    "runId": "",
                    "groupId": "market"
                  }
                }
                """));
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void shouldNotPublishWhenRegistrationRejected() throws Exception {
        String registrationRejectedJson = """
                {
                  "eventId": "reg-res-2",
                  "eventType": "account.registration.result",
                  "schemaVersion": "1.0",
                  "occurredAt": "2026-09-24T08:00:01Z",
                  "correlationId": "reg-antipin-aa",
                  "causationId": "reg-req-1",
                  "producer": "registration-service",
                  "payload": {
                    "secretKey": "bad-key",
                    "accountId": "antipin-aa",
                    "groupId": "МПБ-401",
                    "strategyId": "rsi-reversal",
                    "serviceName": "antipin_service_for_dodeb",
                    "status": "REJECTED",
                    "reason": "Invalid secretKey"
                  }
                }
                """;
        listenerService.onRegistrationResult(registrationRejectedJson);
        assertFalse(listenerService.isRegistered());

        String sampleCandleJson;
        try (InputStream is = getClass().getResourceAsStream("/sample-candle-closed-event.json")) {
            assertNotNull(is);
            sampleCandleJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertDoesNotThrow(() -> listenerService.onCandleClosed(sampleCandleJson));
        verifyNoInteractions(kafkaTemplate);
    }
}
