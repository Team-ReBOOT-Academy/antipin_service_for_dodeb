package ru.antipin.strategy.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ComponentStatus(
        String status,
        String details
) {
    public static ComponentStatus up() {
        return new ComponentStatus("UP", null);
    }
    public static ComponentStatus down(String details) {
        return new ComponentStatus("DOWN", details);
    }
}