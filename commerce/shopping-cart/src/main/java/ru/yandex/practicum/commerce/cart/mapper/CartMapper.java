package ru.yandex.practicum.commerce.cart.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.cart.model.CartItem;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class CartMapper {

    public ShoppingCartDto toDto(ShoppingCart cart) {
        if (cart == null) {
            return null;
        }

        // ⬇️ ИЗМЕНЕНО: теперь Map<UUID, Long> вместо Integer
        Map<UUID, Long> products = cart.getItems().stream()
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        item -> item.getQuantity().longValue()  // Конвертируем Integer в Long
                ));

        return ShoppingCartDto.builder()
                .shoppingCartId(cart.getId())
                .userId(cart.getUserId())
                .products(products)  // теперь Map<UUID, Long>
                .active(cart.isActive())
                .build();
    }

    // ⬇️ ИЗМЕНЕНО: возвращаем Map<UUID, Long>
    public Map<UUID, Long> toProductMap(ShoppingCart cart) {
        if (cart == null || cart.getItems() == null) {
            return new HashMap<>();
        }

        return cart.getItems().stream()
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        item -> item.getQuantity().longValue()  // Конвертируем Integer в Long
                ));
    }
}
