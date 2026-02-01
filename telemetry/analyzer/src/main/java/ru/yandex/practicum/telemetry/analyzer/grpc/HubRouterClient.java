package ru.yandex.practicum.telemetry.analyzer.grpc;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Action;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.entity.Sensor;

import java.time.Instant;

@Service
@Slf4j
public class HubRouterClient {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendDeviceRequest(ScenarioAction scenarioAction) {
        try {
            DeviceActionRequest deviceActionRequest = toDeviceActionRequest(scenarioAction);

            log.info("Sending gRPC request to hub-router: hubId={}, scenario={}, sensor={}, action={}, value={}",
                    deviceActionRequest.getHubId(),
                    deviceActionRequest.getScenarioName(),
                    deviceActionRequest.getAction().getSensorId(),
                    deviceActionRequest.getAction().getType(),
                    deviceActionRequest.getAction().hasValue() ? deviceActionRequest.getAction().getValue() : "null");

            hubRouterClient.handleDeviceAction(deviceActionRequest);

            log.info("✓ Successfully sent device action for scenario: {} to sensor: {}",
                    scenarioAction.getScenario().getName(),
                    scenarioAction.getSensor().getId());

        } catch (Exception e) {
            log.error("❌ Error occurred while sending request for scenario: {}",
                    scenarioAction.getScenario().getName(), e);
        }
    }

    private ActionTypeProto toActionTypeProto(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
            default -> {
                log.error("Unknown action type: {}", actionType);
                throw new IllegalArgumentException("Unknown action type: " + actionType);
            }
        };
    }

    private DeviceActionRequest toDeviceActionRequest(ScenarioAction scenarioAction) {
        Scenario scenario = scenarioAction.getScenario();
        Sensor sensor = scenarioAction.getSensor();
        Action action = scenarioAction.getAction();

        // Строим DeviceActionProto
        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(sensor.getId())
                .setType(toActionTypeProto(action.getType()));

        // Устанавливаем value только если оно есть (optional в proto)
        if (action.getValue() != null) {
            actionBuilder.setValue(action.getValue());
        }

        // Строим DeviceActionRequest
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