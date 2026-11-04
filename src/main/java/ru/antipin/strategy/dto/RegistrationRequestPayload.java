package ru.antipin.strategy.dto;

public record RegistrationRequestPayload(
        String secretKey,
        String accountId,
        String accountName,
        String groupId,
        String strategyId,
        String strategyName,
        String serviceName,
        String description
) {
}
