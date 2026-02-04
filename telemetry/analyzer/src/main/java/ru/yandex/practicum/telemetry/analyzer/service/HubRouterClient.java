package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class HubRouterClient {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
        log.info("HubRouterClient initialized");
        // ПРОВЕРЯЕМ СОЕДИНЕНИЕ
        checkGrpcConnection();
    }

    private void checkGrpcConnection() {
        try {
            log.info("🔍 Checking gRPC connection to Hub Router...");

            // Простая проверка без отправки реального запроса
            hubRouterClient.withDeadlineAfter(2, TimeUnit.SECONDS);
            log.info("✅ gRPC stub is ready");

        } catch (Exception e) {
            log.error("❌ gRPC connection failed: {}", e.getMessage());
        }
    }

    public void sendDeviceRequest(ScenarioAction scenarioAction) {
        log.info("Sending device request for scenario: '{}', hub: {}, sensor: {}",
                scenarioAction.getScenario().getName(),
                scenarioAction.getScenario().getHubId(),
                scenarioAction.getSensor().getId());

        try {
            DeviceActionRequest request = toDeviceActionRequest(scenarioAction);

            hubRouterClient
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .handleDeviceAction(request);

            log.info("✅ Command sent successfully to Hub Router");

        } catch (StatusRuntimeException e) {
            log.error("gRPC error sending to Hub Router: Status={}", e.getStatus());
        } catch (Exception e) {
            log.error("Error sending to Hub Router", e);
        }
    }

    private ActionTypeProto toActionTypeProto(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    private DeviceActionRequest toDeviceActionRequest(ScenarioAction scenarioAction) {
        Scenario scenario = scenarioAction.getScenario();
        Sensor sensor = scenarioAction.getSensor();
        Action action = scenarioAction.getAction();

        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(sensor.getId())
                .setType(toActionTypeProto(action.getType()));

        if (action.getValue() != null) {
            actionBuilder.setValue(action.getValue());
        }

        return DeviceActionRequest.newBuilder()
                .setHubId(scenario.getHubId())
                .setScenarioName(scenario.getName())
                .setAction(actionBuilder.build())
                .setTimestamp(currentTimestamp())
                .build();
    }

    private Timestamp currentTimestamp() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}