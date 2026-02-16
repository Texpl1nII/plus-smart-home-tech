package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.*;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse")
public interface WarehouseClient {

    @PostMapping("/api/v1/warehouse/check-availability")
    ProductAvailabilityResponse checkAvailability(@RequestBody ProductAvailabilityRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();
}