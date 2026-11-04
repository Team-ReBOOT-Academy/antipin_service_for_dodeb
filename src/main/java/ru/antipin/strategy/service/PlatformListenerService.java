package ru.antipin.strategy.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.antipin.strategy.dto.MarketCandleClosedPayload;
import ru.antipin.strategy.dto.RegistrationResultPayload;
import ru.antipin.strategy.event.EventEnvelope;

@Service
public class PlatformListenerService {
    private static final Logger log = LoggerFactory.getLogger(PlatformListenerService.class);
    private final ObjectMapper objectMapper;
    private final String accountId;
    private volatile boolean registered = false;
    public PlatformListenerService(
            ObjectMapper objectMapper,
            @Value("${app.strategy.account-id}") String accountId
    ) {
        this.objectMapper = objectMapper;
        this.accountId = accountId;
    }
    public boolean isRegistered() {
        return registered;
    }
    @KafkaListener(topics = "${app.kafka.topics.registration-result}")
    public void onRegistrationResult(String message) {

        try{
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
                log.info("Успешная регистрация на платформе! Сервис {} авторизован. Причина: {}",
                        payload.serviceName(), payload.reason());
            } else {
                this.registered = false;
                log.error("ОТКАЗ В РЕГИСТРАЦИИ! Статус: {}, Причина: {}",
                        payload.status(), payload.reason());
            }
        } catch (Exception e) {
            log.warn("Не удалось разобрать сообщение результата регистрации: {}", e.getMessage());
        }
    }
    @KafkaListener(topics = "${app.kafka.topics.candles}")
    public void onCandleClosed(String message) {
        try {
            EventEnvelope<MarketCandleClosedPayload> envelope = objectMapper.readValue(
                    message,
                    new TypeReference<>() {}
            );
            MarketCandleClosedPayload candle = envelope.payload();
            if (candle == null) return;
            log.info("Новая свеча [{}]: seq={}, время={}, Open={}, Close={}, High={}, Low={}, Vol={}",
                    candle.instrument() != null ? candle.instrument().ticker() : "UNKNOWN",
                    candle.sequenceNumber(),
                    candle.candleTime(),
                    candle.open(),
                    candle.close(),
                    candle.high(),
                    candle.low(),
                    candle.volume()
            );
        } catch (Exception e) {
            log.warn("Ошибка при обработке входящей свечи: {}", e.getMessage());
        }
    }
}
