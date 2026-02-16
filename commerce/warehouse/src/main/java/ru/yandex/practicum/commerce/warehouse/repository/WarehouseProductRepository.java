package ru.yandex.practicum.commerce.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@Repository
public interface WarehouseProductRepository extends JpaRepository<WarehouseProduct, UUID> {

    List<WarehouseProduct> findByProductIdIn(Set<UUID> productIds);
}