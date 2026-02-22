package ru.yandex.practicum.commerce.cart.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.client.WarehouseClient;
import ru.yandex.practicum.commerce.cart.exception.CartNotActiveException;
import ru.yandex.practicum.commerce.cart.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.cart.mapper.CartMapper;
import ru.yandex.practicum.commerce.cart.model.CartItem;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;
import ru.yandex.practicum.commerce.cart.repository.CartItemRepository;
import ru.yandex.practicum.commerce.cart.repository.ShoppingCartRepository;
import ru.yandex.practicum.commerce.cart.service.CartService;
import ru.yandex.practicum.commerce.dto.AddProductToCartRequest;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityResponse;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CartServiceImpl implements CartService {

    private final ShoppingCartRepository cartRepository;
    private final CartItemRepository itemRepository;
    private final CartMapper cartMapper;
    private final WarehouseClient warehouseClient;

    @Override
    public ShoppingCartDto getCart(String username) {
        log.info("Getting cart for user: {}", username);
        ShoppingCart cart = getOrCreateCart(username);
        return cartMapper.toDto(cart);
    }

    @Override
    @Transactional
    public ShoppingCartDto addProductToCart(String username, AddProductToCartRequest request) {
        log.info("Adding product {} to cart for user: {}", request.getProductId(), username);

        ShoppingCart cart = getActiveCart(username);

        checkAvailability(username, request.getProductId(), request.getQuantity());

        CartItem existingItem = itemRepository.findByCartAndProductId(cart, request.getProductId())
                .orElse(null);

        if (existingItem != null) {
            existingItem.setQuantity(existingItem.getQuantity() + request.getQuantity());
            itemRepository.save(existingItem);
            log.info("Updated quantity for product {}: {}", request.getProductId(), existingItem.getQuantity());
        } else {
            CartItem newItem = CartItem.builder()
                    .cart(cart)
                    .productId(request.getProductId())
                    .quantity(request.getQuantity())
                    .price(0.0)
                    .build();
            cart.getItems().add(newItem);
            itemRepository.save(newItem);
            log.info("Added new product {} to cart", request.getProductId());
        }

        return cartMapper.toDto(cart);
    }

    @Override
    @Transactional
    public void removeProductFromCart(String username, UUID productId) {
        log.info("Removing product {} from cart for user: {}", productId, username);

        ShoppingCart cart = getActiveCart(username);

        CartItem item = itemRepository.findByCartAndProductId(cart, productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found in cart: " + productId));

        cart.getItems().remove(item);
        itemRepository.delete(item);

        log.info("Product removed from cart");
    }

    @Override
    @Transactional
    public void updateProductQuantity(String username, UUID productId, Integer newQuantity) {
        log.info("Updating quantity for product {} to {} for user: {}", productId, newQuantity, username);

        ShoppingCart cart = getActiveCart(username);

        CartItem item = itemRepository.findByCartAndProductId(cart, productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found in cart: " + productId));

        checkAvailability(username, productId, newQuantity);

        item.setQuantity(newQuantity);
        itemRepository.save(item);

        log.info("Quantity updated successfully");
    }

    @Override
    @Transactional
    public void clearCart(String username) {
        log.info("Clearing cart for user: {}", username);

        ShoppingCart cart = getActiveCart(username);

        itemRepository.deleteByCart(cart);
        cart.getItems().clear();

        log.info("Cart cleared");
    }

    @Override
    @Transactional
    public void deactivateCart(String username) {
        log.info("Deactivating cart for user: {}", username);

        ShoppingCart cart = cartRepository.findByUserId(username)
                .orElseThrow(() -> new CartNotActiveException("Cart not found for user: " + username));

        cart.setActive(false);
        cartRepository.save(cart);

        log.info("Cart deactivated");
    }

    @Override
    @Transactional
    public ShoppingCartDto addProductsToCart(String username, Map<UUID, Long> products) {
        log.info("Adding multiple products to cart for user: {}", username);

        ShoppingCart cart = getActiveCart(username);

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            Integer quantity = entry.getValue().intValue();

            checkAvailability(username, productId, quantity);

            CartItem existingItem = itemRepository.findByCartAndProductId(cart, productId)
                    .orElse(null);

            if (existingItem != null) {
                existingItem.setQuantity(existingItem.getQuantity() + quantity);
                itemRepository.save(existingItem);
            } else {
                CartItem newItem = CartItem.builder()
                        .cart(cart)
                        .productId(productId)
                        .quantity(quantity)
                        .price(0.0)
                        .build();
                cart.getItems().add(newItem);
                itemRepository.save(newItem);
            }
        }

        return cartMapper.toDto(cart);
    }

    @Override
    @Transactional
    public ShoppingCartDto removeProductsFromCart(String username, List<UUID> productIds) {
        log.info("Removing multiple products from cart for user: {}", username);

        ShoppingCart cart = getActiveCart(username);

        for (UUID productId : productIds) {
            itemRepository.findByCartAndProductId(cart, productId)
                    .ifPresent(item -> {
                        cart.getItems().remove(item);
                        itemRepository.delete(item);
                    });
        }

        return cartMapper.toDto(cart);
    }

    @Override
    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        log.info("Changing quantity for product {} to {} for user: {}",
                request.getProductId(), request.getNewQuantity(), username);

        ShoppingCart cart = getActiveCart(username);

        CartItem item = itemRepository.findByCartAndProductId(cart, request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found in cart: " + request.getProductId()));

        checkAvailability(username, request.getProductId(), request.getNewQuantity().intValue());

        item.setQuantity(request.getNewQuantity().intValue());
        itemRepository.save(item);

        return cartMapper.toDto(cart);
    }

    private ShoppingCart getActiveCart(String username) {
        log.info("Looking for active cart for user: {}", username);

        return cartRepository.findByUserIdAndActiveTrue(username)
                .orElseThrow(() -> new CartNotActiveException(
                        "No active cart found for user: " + username));
    }

    private ShoppingCart getOrCreateCart(String username) {
        return cartRepository.findByUserId(username)
                .orElseGet(() -> createNewCart(username));
    }

    @Transactional
    protected ShoppingCart createNewCart(String username) {
        log.info("Creating new cart for user: {}", username);

        ShoppingCart newCart = ShoppingCart.builder()
                .userId(username)
                .active(true)
                .build();

        ShoppingCart savedCart = cartRepository.save(newCart);
        log.info("Created new cart with id: {}", savedCart.getId());

        return savedCart;
    }

    private void checkAvailability(String username, UUID productId, Integer quantity) {
        Map<UUID, Integer> products = new HashMap<>();
        products.put(productId, quantity);

        ProductAvailabilityRequest request = ProductAvailabilityRequest.builder()
                .products(products)
                .username(username)
                .build();

        ProductAvailabilityResponse response = warehouseClient.checkAvailability(request);

        if (!response.isAvailable()) {
            throw new ProductNotFoundException(
                    "Product not available in warehouse: " + productId);
        }
    }
}