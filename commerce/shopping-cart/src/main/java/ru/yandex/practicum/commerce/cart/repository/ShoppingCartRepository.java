package ru.yandex.practicum.commerce.cart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ShoppingCartRepository extends JpaRepository<ShoppingCart, UUID> {  // ← UUID вместо String

    Optional<ShoppingCart> findByUserIdAndActiveTrue(String userId);

    Optional<ShoppingCart> findByUserId(String userId);
}