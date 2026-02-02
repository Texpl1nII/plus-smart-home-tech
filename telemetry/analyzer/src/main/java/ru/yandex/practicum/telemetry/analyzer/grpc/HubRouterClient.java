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
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;

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
            log.info("🚀 SENDING TO HUB ROUTER: Scenario={}, Hub={}, Sensor={}, Action={}, Value={}",
                    scenarioAction.getScenario().getName(),
                    scenarioAction.getScenario().getHubId(),
                    scenarioAction.getSensor().getId(),
                    scenarioAction.getAction().getType(),
                    scenarioAction.getAction().getValue());

            DeviceActionRequest deviceActionRequest = toDeviceActionRequest(scenarioAction);
            hubRouterClient.handleDeviceAction(deviceActionRequest);

            log.info("✅ SENT TO HUB ROUTER");
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

        return DeviceActionRequest.newBuilder()
                .setHubId(scenario.getHubId())
                .setScenarioName(scenario.getName())
                .setAction(DeviceActionProto.newBuilder()
                        .setSensorId(sensor.getId())
                        .setType(toActionTypeProto(action.getType()))
                        .setValue(action.getValue())
                        .build())
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