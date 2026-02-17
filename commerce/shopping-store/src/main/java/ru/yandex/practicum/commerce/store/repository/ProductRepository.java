package ru.yandex.practicum.commerce.store.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.store.model.Product;

import java.util.List;
import java.util.UUID;

@Repository
public interface ProductRepository extends JpaRepository<Product, UUID> {

    // Для пагинации (НОВЫЙ)
    Page<Product> findByCategoryAndStatus(ProductCategory category, ProductStatus status, Pageable pageable);

    // Для списка (старый, если нужен)
    List<Product> findByCategoryAndStatus(ProductCategory category, ProductStatus status);

    List<Product> findByStatus(ProductStatus status);
}