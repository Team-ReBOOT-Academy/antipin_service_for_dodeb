package ru.antipin.strategy.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.antipin.strategy.dto.MarketCandleClosedPayload;
import ru.antipin.strategy.dto.RecommendationCreatedPayload;
import ru.antipin.strategy.dto.RegistrationResultPayload;
import ru.antipin.strategy.event.EventEnvelope;

@Service
public class PlatformListenerService {
    private static final Logger log = LoggerFactory.getLogger(PlatformListenerService.class);
    private final ObjectMapper objectMapper;
    private final RecommendationService recommendationService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String accountId;
    private final String recommendationsTopic;
    private volatile boolean registered = false;
    private volatile boolean registrationRejected = false;

    public PlatformListenerService(
            ObjectMapper objectMapper,
            RecommendationService recommendationService,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.strategy.account-id}") String accountId,
            @Value("${app.kafka.topics.recommendations}") String recommendationsTopic
    ) {
        this.objectMapper = objectMapper;
        this.recommendationService = recommendationService;
        this.kafkaTemplate = kafkaTemplate;
        this.accountId = accountId;
        this.recommendationsTopic = recommendationsTopic;
    }

    public boolean isRegistered() {
        return registered;
    }

    @KafkaListener(topics = "${app.kafka.topics.registration-result}")
    public void onRegistrationResult(String message) {
        try {
            EventEnvelope<RegistrationResultPayload> envelope = objectMapper.readValue(
                    message,
                    new TypeReference<>() {}
            );
            RegistrationResultPayload payload = envelope.payload();

            if (payload == null || !accountId.equals(payload.accountId())) {
                return;
            }
            if ("REGISTERED".equalsIgnoreCase(payload.status())) {
                this.registered = true;
                this.registrationRejected = false;
                log.info("Успешная регистрация на платформе! Сервис {} авторизован. Причина: {}",
                        payload.serviceName(), payload.reason());
            } else {
                this.registered = false;
                this.registrationRejected = true;
                log.error("ОТКАЗ В РЕГИСТРАЦИИ! Статус: {}, Причина: {}",
                        payload.status(), payload.reason());
            }
        } catch (Exception e) {
            log.warn("Не удалось разобрать сообщение результата регистрации: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "${app.kafka.topics.candles}")
    public void onCandleClosed(String message) {
        long startNanos = System.nanoTime();
        try {
            EventEnvelope<MarketCandleClosedPayload> envelope = objectMapper.readValue(
                    message,
                    new TypeReference<>() {}
            );
            EventEnvelope<RecommendationCreatedPayload> recommendationEnvelope =
                    recommendationService.processCandle(envelope, startNanos);

            MarketCandleClosedPayload candle = envelope.payload();
            RecommendationCreatedPayload recommendation = recommendationEnvelope.payload();

            log.info("Принята свеча [{}]: runId={}, seq={}, время={}, Open={}, Close={}, High={}, Low={}, Vol={}",
                    recommendation.instrument(),
                    candle.runId(),
                    candle.sequenceNumber(),
                    candle.candleTime(),
                    candle.open(),
                    candle.close(),
                    candle.high(),
                    candle.low(),
                    candle.volume()
            );

            if (registrationRejected) {
                log.warn("Публикация рекомендации пропущена: регистрация сервиса отклонена платформой.");
                return;
            }
            if (!registered) {
                log.warn("Статус REGISTERED в текущем запуске еще не подтвержден; отправляем рекомендацию по активной регистрации аккаунта.");
            }

            String recommendationJson = objectMapper.writeValueAsString(recommendationEnvelope);
            kafkaTemplate.send(recommendationsTopic, accountId, recommendationJson)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Ошибка доставки рекомендации [{} seq={}] в Kafka: {}",
                                    recommendation.instrument(), recommendation.sequenceNumber(), ex.getMessage());
                        } else {
                            log.info("Отправлена рекомендация [{}]: runId={}, seq={}, signal={}, id={}, timeMs={}",
                                    recommendation.instrument(),
                                    recommendation.runId(),
                                    recommendation.sequenceNumber(),
                                    recommendation.signal(),
                                    recommendation.recommendationId(),
                                    recommendation.processingTimeMs());
                        }
                    });
        } catch (Exception e) {
            log.warn("Некорректное сообщение свечи отклонено (слушатель продолжает работу): {}", e.getMessage());
        }
    }
}
