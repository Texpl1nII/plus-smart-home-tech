package ru.yandex.practicum.commerce.cart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.cart.model.CartItem;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CartItemRepository extends JpaRepository<CartItem, UUID> {

    List<CartItem> findByCart(ShoppingCart cart);

    Optional<CartItem> findByCartAndProductId(ShoppingCart cart, UUID productId);

    void deleteByCart(ShoppingCart cart);
}
