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

    @GetMapping
    public ResponseEntity<PageProductDto> getProducts(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") String[] sort) {

        log.info("GET /?category={}&page={}&size={}&sort={}", category, page, size, (Object[]) sort);

        Pageable pageable = PageRequest.of(page, size);
        Page<ProductDto> productPage = storeService.getProductsByCategory(category, pageable);

        PageProductDto response = convertToPageProductDto(productPage);

        return ResponseEntity.ok(response);
    }

    private PageProductDto convertToPageProductDto(Page<ProductDto> page) {
        if (page == null || page.isEmpty()) {
            return PageProductDto.builder()
                    .content(new ArrayList<>())
                    .totalElements(0)
                    .totalPages(0)
                    .size(0)
                    .number(0)
                    .first(true)
                    .last(true)
                    .empty(true)
                    .sort(PageProductDto.SortObject.builder().build())
                    .pageable(PageProductDto.PageableObject.builder()
                            .offset(0)
                            .pageNumber(0)
                            .pageSize(0)
                            .paged(false)
                            .unpaged(true)
                            .sort(new ArrayList<>())
                            .build())
                    .build();
        }

        List<PageProductDto.SortObject> sortObjects = page.getSort().stream()
                .map(order -> PageProductDto.SortObject.builder()
                        .direction(order.getDirection().name())
                        .property(order.getProperty())
                        .ascending(order.isAscending())
                        .ignoreCase(order.isIgnoreCase())
                        .build())
                .collect(Collectors.toList());

        PageProductDto.SortObject mainSort = sortObjects.isEmpty()
                ? PageProductDto.SortObject.builder().build()
                : sortObjects.get(0);

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
                .sort(mainSort)
                .pageable(pageableObject)
                .build();
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

    private Sort parseSort(String[] sort) {
        if (sort == null || sort.length == 0) {
            return Sort.by("name").ascending();
        }

        List<Sort.Order> orders = new ArrayList<>();
        for (String sortParam : sort) {
            String[] parts = sortParam.split(",");
            String property = parts[0];

            if ("productName".equals(property)) {
                property = "name";
            } else if ("productCategory".equals(property)) {
                property = "category";
            } else if ("productState".equals(property)) {
                property = "status";
            } else if ("quantityState".equals(property)) {
                property = "availability";
            }

            Sort.Direction direction = Sort.Direction.ASC;
            if (parts.length > 1) {
                String directionStr = parts[1].toLowerCase();
                if ("desc".equals(directionStr)) {
                    direction = Sort.Direction.DESC;
                }
            }

            orders.add(new Sort.Order(direction, property).ignoreCase());
        }

        return Sort.by(orders);
    }
}
