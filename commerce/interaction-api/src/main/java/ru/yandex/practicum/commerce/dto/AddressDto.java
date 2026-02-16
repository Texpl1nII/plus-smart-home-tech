package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AddressDto {
    private String country;
    private String city;
    private String street;
    private String house;

    @JsonProperty("apartment")  // для Jackson
    private String apartment;

    @JsonProperty("flat")       // тоже для обратной совместимости
    public String getFlat() {
        return apartment;
    }
}
