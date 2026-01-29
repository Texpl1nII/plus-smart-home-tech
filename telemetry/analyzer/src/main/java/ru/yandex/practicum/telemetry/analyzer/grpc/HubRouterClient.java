package ru.yandex.practicum.telemetry.analyzer.grpc;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.telemetry.analyzer.entity.ActionType;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;

import java.time.Instant;

@Slf4j
@Service
public class HubRouterClient {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendDeviceRequest(ScenarioAction scenarioAction) {
        try {
            DeviceActionRequest request = createDeviceActionRequest(scenarioAction);
            hubRouterClient.handleDeviceAction(request);

            log.debug("Sent device action for scenario: {} to sensor: {}",
                    scenarioAction.getScenario().getName(),
                    scenarioAction.getSensor().getId());
        } catch (Exception e) {
            log.error("Error sending device request for scenario: {}",
                    scenarioAction.getScenario().getName(), e);
            throw new RuntimeException("Failed to send device request", e);
        }
    }

    private DeviceActionRequest createDeviceActionRequest(ScenarioAction scenarioAction) {
        return DeviceActionRequest.newBuilder()
                .setHubId(scenarioAction.getScenario().getHubId())
                .setScenarioName(scenarioAction.getScenario().getName())
                .setAction(DeviceActionProto.newBuilder()
                        .setSensorId(scenarioAction.getSensor().getId())
                        .setType(convertActionType(scenarioAction.getAction().getType()))
                        .setValue(scenarioAction.getAction().getValue())
                        .build())
                .setTimestamp(currentTimestamp())
                .build();
    }

    private ActionTypeProto convertActionType(ActionType actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    private Timestamp currentTimestamp() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
