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
import ru.yandex.practicum.commerce.client.StoreClient;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.store.PageProductDto;
import ru.yandex.practicum.commerce.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.commerce.store.service.StoreService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class StoreController implements StoreClient {

    private final StoreService storeService;

    @Override
    @GetMapping("/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.info("GET /{}", productId);
        return storeService.getProductById(productId);
    }

    @Override
    @GetMapping
    public List<ProductDto> getProductsByCategory(@RequestParam("category") ProductCategory category) {
        log.info("GET with category: {}", category);
        return storeService.getProductsByCategoryOld(category);
    }

    @GetMapping(params = {"category", "page", "size", "sort"})
    public ResponseEntity<PageProductDto> getProductsWithPagination(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") String[] sort) {

        log.info("GET with pagination - category={}&page={}&size={}&sort={}",
                category, page, size, (Object[]) sort);

        // Используем parseSortCorrectly для создания сортировки из параметров
        Sort sortObject = parseSortCorrectly(sort);
        Pageable pageable = PageRequest.of(page, size, sortObject);

        Page<ProductDto> productPage = storeService.getProductsByCategory(category, pageable);

        // Конвертируем в PageProductDto с правильными полями сортировки
        PageProductDto response = convertToPageProductDto(productPage, sort, sortObject);

        return ResponseEntity.ok(response);
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
        log.info("----- INSIDE parseSortCorrectly -----");
        log.info("Input: {}", (Object[]) sort);

        if (sort == null || sort.length == 0) {
            log.info("No sort, using default ASC");
            return Sort.by("name").ascending();
        }

        String sortParam = sort[0];
        log.info("Using sortParam[0]: '{}'", sortParam);

        String[] parts = sortParam.split(",");
        log.info("Split into {} parts: {}", parts.length, (Object[]) parts);

        String field = parts[0];
        String direction = parts.length > 1 ? parts[1] : "asc";

        log.info("Parsed - field: '{}', direction: '{}'", field, direction);

        String jpaField = "name";

        Sort.Order order;
        if ("desc".equalsIgnoreCase(direction)) {
            log.info(">>> CREATING DESC ORDER for field: {}", jpaField);
            order = Sort.Order.desc(jpaField);
        } else {
            log.info(">>> CREATING ASC ORDER for field: {}", jpaField);
            order = Sort.Order.asc(jpaField);
        }

        Sort result = Sort.by(order);
        log.info("Result sort: {}", result);
        log.info("----- EXIT parseSortCorrectly -----");
        return result;
    }

    private PageProductDto convertToPageProductDto(Page<ProductDto> page, String[] sortParams, Sort sort) {
        log.info("Converting to PageProductDto");

        String sortParam = sortParams[0];
        String[] parts = sortParam.split(",");
        String originalField = parts[0];
        String direction = parts.length > 1 ? parts[1] : "asc";

        List<PageProductDto.SortObject> sortObjects = new ArrayList<>();
        PageProductDto.SortObject sortObj = PageProductDto.SortObject.builder()
                .direction(direction.toUpperCase())
                .property(originalField)
                .ascending(!"desc".equalsIgnoreCase(direction))
                .ignoreCase(false)
                .sorted(true)
                .unsorted(false)
                .empty(false)
                .build();
        sortObjects.add(sortObj);

        List<PageProductDto.SortObject> pageableSortObjects = new ArrayList<>();
        PageProductDto.SortObject pageableSortObj = PageProductDto.SortObject.builder()
                .direction(direction.toUpperCase())
                .property(originalField)
                .ascending(!"desc".equalsIgnoreCase(direction))
                .ignoreCase(false)
                .sorted(true)
                .unsorted(false)
                .empty(false)
                .build();
        pageableSortObjects.add(pageableSortObj);

        PageProductDto.PageableObject pageableObject = PageProductDto.PageableObject.builder()
                .offset(page.getPageable().getOffset())
                .pageNumber(page.getPageable().getPageNumber())
                .pageSize(page.getPageable().getPageSize())
                .paged(page.getPageable().isPaged())
                .unpaged(page.getPageable().isUnpaged())
                .sort(pageableSortObjects)
                .sorted(true)
                .unsorted(false)
                .build();

        return PageProductDto.builder()
                .content(page.getContent())
                .totalPages(page.getTotalPages())
                .totalElements(page.getTotalElements())
                .size(page.getSize())
                .number(page.getNumber())
                .first(page.isFirst())
                .last(page.isLast())
                .empty(page.isEmpty())
                .sort(sortObjects)
                .pageable(pageableObject)
                .numberOfElements(page.getNumberOfElements())
                .hasContent(page.hasContent())
                .hasNext(page.hasNext())
                .hasPrevious(page.hasPrevious())
                .isFirst(page.isFirst())
                .isLast(page.isLast())
                .build();
    }
}
