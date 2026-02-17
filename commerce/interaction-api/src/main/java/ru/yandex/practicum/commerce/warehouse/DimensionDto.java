package ru.yandex.practicum.commerce.warehouse;

import lombok.Builder;
import lombok.Data;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

@Data
@Builder
public class DimensionDto {

    @NotNull(message = "Ширина обязательна")
    @Positive(message = "Ширина должна быть положительной")
    private Double width;

    @NotNull(message = "Высота обязательна")
    @Positive(message = "Высота должна быть положительной")
    private Double height;

    @NotNull(message = "Глубина обязательна")
    @Positive(message = "Глубина должна быть положительной")
    private Double depth;
}