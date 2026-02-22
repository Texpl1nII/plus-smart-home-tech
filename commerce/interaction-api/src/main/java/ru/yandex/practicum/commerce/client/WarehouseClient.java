package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.cart.config.FeignConfig;
import ru.yandex.practicum.commerce.dto.*;

@FeignClient(name = "WAREHOUSE", configuration = FeignConfig.class)
public interface WarehouseClient {

    @PostMapping("/api/v1/warehouse/check")
    ProductAvailabilityResponse checkAvailability(@RequestBody ProductAvailabilityRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();
}