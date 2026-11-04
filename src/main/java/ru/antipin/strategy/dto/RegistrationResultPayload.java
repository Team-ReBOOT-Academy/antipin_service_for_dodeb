package ru.antipin.strategy.dto;

public record RegistrationResultPayload(
        String secretKey,
        String accountId,
        String groupId,
        String strategyId,
        String serviceName,
        String status,
        String reason
) {
}
