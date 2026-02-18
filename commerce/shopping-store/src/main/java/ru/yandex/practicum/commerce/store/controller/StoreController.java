package ru.yandex.practicum.commerce.store.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.store.PageProductDto;
import ru.yandex.practicum.commerce.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.commerce.store.service.StoreService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class StoreController {

    private final StoreService storeService;

    // Эндпоинт для GET с пагинацией (использует кастомный DTO с content)
    @GetMapping(params = {"category", "page", "size", "sort"})
    public ResponseEntity<PageProductDto> getProductsWithPagination(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") String[] sort) {

        log.info("GET with pagination - category={}&page={}&size={}&sort={}",
                category, page, size, (Object[]) sort);

        // Исправляем парсинг сортировки - создаем Sort объект правильно
        Sort sortBy = parseSortCorrectly(sort);
        Pageable pageable = PageRequest.of(page, size, sortBy);
        Page<ProductDto> productPage = storeService.getProductsByCategory(category, pageable);

        PageProductDto response = convertToPageProductDto(productPage, sortBy);

        return ResponseEntity.ok(response);
    }

    // Эндпоинт для GET только с category (возвращает список)
    @GetMapping(params = "category")
    public ResponseEntity<List<ProductDto>> getProductsByCategoryOnly(
            @RequestParam ProductCategory category) {
        log.info("GET with category only: {}", category);
        List<ProductDto> products = storeService.getProductsByCategoryOld(category);
        return ResponseEntity.ok(products);
    }

    // Эндпоинт для GET без параметров
    @GetMapping
    public ResponseEntity<String> getDefault() {
        return ResponseEntity.badRequest().body("Category parameter is required");
    }

    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createNewProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("PUT / - creating product: {}", productDto.getProductName());
        return storeService.createProduct(productDto);
    }

    @PostMapping
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("POST / - updating product: {}", productDto.getProductId());

        if (productDto.getProductId() == null) {
            throw new IllegalArgumentException("Product ID must not be null for update");
        }

        return storeService.updateProduct(productDto.getProductId(), productDto);
    }

    @GetMapping("/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.info("GET /{}", productId);
        return storeService.getProductById(productId);
    }

    @PostMapping("/removeProductFromStore")
    public boolean removeProductFromStore(@RequestBody UUID productId) {
        log.info("POST /removeProductFromStore - removing product: {}", productId);
        return storeService.deactivateProduct(productId);
    }

    @PostMapping(value = "/quantityState", consumes = MediaType.APPLICATION_JSON_VALUE)
    public boolean setProductQuantityStateWithBody(@Valid @RequestBody SetProductQuantityStateRequest request) {
        log.info("POST /quantityState (body) - product: {}, state: {}",
                request.getProductId(), request.getQuantityState());
        return storeService.setProductQuantityState(request);
    }

    @PostMapping(value = "/quantityState", params = {"productId", "quantityState"})
    public boolean setProductQuantityStateWithParams(
            @RequestParam UUID productId,
            @RequestParam AvailabilityStatus quantityState) {
        log.info("POST /quantityState (params) - product: {}, state: {}", productId, quantityState);

        SetProductQuantityStateRequest request = SetProductQuantityStateRequest.builder()
                .productId(productId)
                .quantityState(quantityState)
                .build();

        return storeService.setProductQuantityState(request);
    }

    private Sort parseSortCorrectly(String[] sort) {
        log.info("===== SORT DEBUG =====");
        log.info("Raw sort array: {}", (Object[]) sort);

        if (sort == null || sort.length == 0) {
            log.info("No sort parameters, using default: name ASC");
            return Sort.by("name").ascending();
        }

        List<Sort.Order> orders = new ArrayList<>();

        for (String sortParam : sort) {
            log.info("Processing sortParam: '{}'", sortParam);

            String[] parts = sortParam.split(",");
            log.info("Split into {} parts: {}", parts.length, (Object[]) parts);

            String field = parts[0];
            log.info("Original field: '{}'", field);

            // Маппинг поля
            if ("productName".equals(field)) {
                field = "name";
                log.info("Mapped productName -> name");
            } else if ("productCategory".equals(field)) {
                field = "category";
                log.info("Mapped productCategory -> category");
            } else if ("productState".equals(field)) {
                field = "status";
                log.info("Mapped productState -> status");
            } else if ("quantityState".equals(field)) {
                field = "availability";
                log.info("Mapped quantityState -> availability");
            }

            // Определяем направление
            if (parts.length > 1 && "desc".equalsIgnoreCase(parts[1])) {
                log.info("Direction: DESC for field: {}", field);
                orders.add(Sort.Order.desc(field));
            } else {
                log.info("Direction: ASC for field: {}", field);
                orders.add(Sort.Order.asc(field));
            }
        }

        Sort result = Sort.by(orders);
        log.info("Final Sort: {}", result);
        log.info("===== END SORT DEBUG =====");
        return result;
    }

    // Конвертер в кастомный DTO
    private PageProductDto convertToPageProductDto(Page<ProductDto> page, Sort sort) {
        List<PageProductDto.SortObject> sortObjects = new ArrayList<>();

        sort.forEach(order -> {
            sortObjects.add(PageProductDto.SortObject.builder()
                    .direction(order.getDirection().name())
                    .property(order.getProperty())
                    .ascending(order.isAscending())
                    .ignoreCase(order.isIgnoreCase())
                    .build());
        });

        PageProductDto.PageableObject pageableObject = PageProductDto.PageableObject.builder()
                .offset(page.getPageable().getOffset())
                .pageNumber(page.getPageable().getPageNumber())
                .pageSize(page.getPageable().getPageSize())
                .paged(page.getPageable().isPaged())
                .unpaged(page.getPageable().isUnpaged())
                .sort(sortObjects)
                .build();

        return PageProductDto.builder()
                .content(page.getContent())
                .totalElements(page.getTotalElements())
                .totalPages(page.getTotalPages())
                .size(page.getSize())
                .number(page.getNumber())
                .first(page.isFirst())
                .last(page.isLast())
                .empty(page.isEmpty())
                .sort(sortObjects)
                .pageable(pageableObject)
                .build();
    }
}
