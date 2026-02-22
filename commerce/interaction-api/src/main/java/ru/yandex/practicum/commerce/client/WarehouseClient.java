package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.*;

@FeignClient(name = "warehouse", url = "http://10.1.0.21:36935")  // адрес из последнего лога!
public interface WarehouseClient {

    @PostMapping("/api/v1/warehouse/check-availability")
    ProductAvailabilityResponse checkAvailability(@RequestBody ProductAvailabilityRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();
}