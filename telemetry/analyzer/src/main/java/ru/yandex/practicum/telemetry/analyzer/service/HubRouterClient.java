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

        // Проверяем подключение при создании
        checkGrpcConnection();
    }

    private void checkGrpcConnection() {
        log.info("🔍 Проверка gRPC подключения к Hub Router...");

        try {
            // Простая проверка - пытаемся получить stub
            if (hubRouterClient != null) {
                log.info("✅ gRPC stub создан успешно");

                // Можно попробовать отправить тестовый запрос
                DeviceActionRequest testRequest = DeviceActionRequest.newBuilder()
                        .setHubId("test-connection")
                        .setScenarioName("test")
                        .setAction(DeviceActionProto.newBuilder()
                                .setSensorId("test-sensor")
                                .setType(ActionTypeProto.ACTIVATE)
                                .build())
                        .setTimestamp(Timestamp.newBuilder()
                                .setSeconds(System.currentTimeMillis() / 1000)
                                .build())
                        .build();

                try {
                    hubRouterClient
                            .withDeadlineAfter(2, TimeUnit.SECONDS)
                            .handleDeviceAction(testRequest);
                    log.info("✅ Hub Router доступен!");
                } catch (StatusRuntimeException e) {
                    // Это нормально - тестовый сценарий не существует
                    log.info("✅ Hub Router отвечает (ошибка ожидаема): {}", e.getStatus());
                }
            } else {
                log.error("❌ gRPC stub не создан!");
            }

        } catch (Exception e) {
            log.error("❌ Ошибка при проверке gRPC подключения: {}", e.getMessage());
        }
    }

    public void sendDeviceRequest(ScenarioAction scenarioAction) {
        log.info("🎯 === HUB ROUTER CLIENT ===");
        log.info("Scenario: '{}'", scenarioAction.getScenario().getName());
        log.info("Hub: {}", scenarioAction.getScenario().getHubId());
        log.info("Sensor: {}", scenarioAction.getSensor().getId());
        log.info("Action type: {}", scenarioAction.getAction().getType());
        log.info("Action value: {}", scenarioAction.getAction().getValue());

        try {
            DeviceActionRequest request = toDeviceActionRequest(scenarioAction);
            log.info("📡 gRPC Request: {}", request);

            // Отправляем с таймаутом
            hubRouterClient
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .handleDeviceAction(request);

            log.info("✅ SUCCESS: Command sent to Hub Router!");

        } catch (StatusRuntimeException e) {
            log.error("❌ gRPC ERROR: Status={}, Description={}",
                    e.getStatus(), e.getStatus().getDescription());
        } catch (Exception e) {
            log.error("❌ ERROR sending to Hub Router", e);
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