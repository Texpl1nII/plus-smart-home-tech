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

        Map<UUID, Integer> products = cart.getItems().stream()
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        CartItem::getQuantity
                ));

        return ShoppingCartDto.builder()
                .shoppingCartId(cart.getId())
                .userId(cart.getUserId())
                .products(products)
                .active(cart.isActive())
                .build();
    }

    public Map<UUID, Integer> toProductMap(ShoppingCart cart) {
        if (cart == null || cart.getItems() == null) {
            return new HashMap<>();
        }

        return cart.getItems().stream()
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        CartItem::getQuantity
                ));
    }
}
