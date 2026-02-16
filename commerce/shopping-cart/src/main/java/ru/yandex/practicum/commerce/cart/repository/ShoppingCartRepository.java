package ru.yandex.practicum.commerce.cart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;

import java.util.Optional;

@Repository
public interface ShoppingCartRepository extends JpaRepository<ShoppingCart, String> {

    Optional<ShoppingCart> findByUserIdAndActiveTrue(String userId);

    Optional<ShoppingCart> findByUserId(String userId);
}