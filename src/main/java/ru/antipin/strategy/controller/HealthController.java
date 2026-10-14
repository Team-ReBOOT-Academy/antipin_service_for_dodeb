package ru.antipin.strategy.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.antipin.strategy.dto.HealthResponse;

import java.util.Collections;

@RestController
public class HealthController {

    private final String serviceName;
    private final String version;

    public HealthController(
            @Value("${app.service-name:antipin_service_for_dodeb}") String serviceName,
            @Value("${app.version:1.0.0}") String version) {
        this.serviceName = serviceName;
        this.version = version;
    }

    @GetMapping(value = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<HealthResponse> getHealth() {
        HealthResponse response = new HealthResponse(
                "UP",
                serviceName,
                version,
                Collections.emptyMap()
        );
        return ResponseEntity.ok(response);
    }
}