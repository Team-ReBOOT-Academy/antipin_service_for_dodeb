package ru.antipin.strategy.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.antipin.strategy.dto.RegistrationRequestPayload;
import ru.antipin.strategy.event.EventEnvelope;

@Service
public class RegistrationService {
    private static final Logger log = LoggerFactory.getLogger(RegistrationService.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final String secretKey;
    private final String accountId;
    private final String accountName;
    private final String groupId;
    private final String strategyId;
    private final String strategyName;
    private final String serviceName;

    public RegistrationService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topics.registration-request}") String topic,
            @Value("${app.strategy.secret-key}") String secretKey,
            @Value("${app.strategy.account-id}") String accountId,
            @Value("${app.strategy.account-name}") String accountName,
            @Value("${app.strategy.group-id}") String groupId,
            @Value("${app.strategy.strategy-id}") String strategyId,
            @Value("${app.strategy.strategy-name}") String strategyName,
            @Value("${app.service-name}") String serviceName
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.secretKey = secretKey;
        this.accountId = accountId;
        this.accountName = accountName;
        this.groupId = groupId;
        this.strategyId = strategyId;
        this.strategyName = strategyName;
        this.serviceName = serviceName;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void sendRegistration(){
        log.info("Запуск процесса регистрации сервиса {} на платформе...", serviceName);
        try{
            RegistrationRequestPayload payload = new RegistrationRequestPayload(
                    secretKey,
                    accountId,
                    accountName,
                    groupId,
                    strategyId,
                    strategyName,
                    serviceName,
                    "RSI Reversal Strategy service"
            );
            EventEnvelope<RegistrationRequestPayload> envelope = EventEnvelope.create(
                    "account.registration.request",
                    "reg-" + accountId,
                    null,
                    serviceName,
                    payload
            );
            String jsonMessage = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(topic, accountId, jsonMessage);
            log.info("Запрос регистрации успешно отправлен в топик '{}': {}", topic, jsonMessage);
        }catch (Exception e){
            log.error("Не удалось отправить запрос регистрации в Kafka: {}", e.getMessage());
        }
    }
}
