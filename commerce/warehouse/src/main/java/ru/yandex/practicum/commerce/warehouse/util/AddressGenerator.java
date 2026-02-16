package ru.yandex.practicum.commerce.warehouse.util;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.AddressDto;

import java.security.SecureRandom;

@Slf4j
@Component
public class AddressGenerator {

    private static final String[] ADDRESSES = new String[] {"ADDRESS_1", "ADDRESS_2"};

    private static String currentAddress;

    @PostConstruct
    public void init() {
        // Выбор случайного адреса при инициализации
        SecureRandom random = new SecureRandom();
        currentAddress = ADDRESSES[random.nextInt(ADDRESSES.length)];
        log.info("Warehouse initialized with address: {}", currentAddress);
    }

    public AddressDto getCurrentAddress() {
        // По ТЗ: строку ADDRESS_1 или ADDRESS_2 продублировать в каждое из полей
        return AddressDto.builder()
                .country(currentAddress)
                .city(currentAddress)
                .street(currentAddress)
                .house(currentAddress)
                .apartment(currentAddress)
                .build();
    }
}
