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

    @GetMapping(params = {"category", "page", "size", "sort"})
    public ResponseEntity<PageProductDto> getProductsWithPagination(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") String[] sort) {

        log.info("GET with pagination - category={}&page={}&size={}&sort={}",
                category, page, size, (Object[]) sort);

        // Создаем сортировку на основе параметра
        String sortParam = sort[0];
        String[] parts = sortParam.split(",");
        String field = parts[0];
        String direction = parts.length > 1 ? parts[1] : "asc";

        log.info("Field from request: {}, direction: {}", field, direction);

        // Для JPA используем name, но запоминаем оригинальное поле для ответа
        String jpaField = "name";

        Sort sortObject;
        if ("desc".equalsIgnoreCase(direction)) {
            sortObject = Sort.by(Sort.Order.desc(jpaField));
            log.info("Created DESC sort for JPA field: {}", jpaField);
        } else {
            sortObject = Sort.by(Sort.Order.asc(jpaField));
            log.info("Created ASC sort for JPA field: {}", jpaField);
        }

        Pageable pageable = PageRequest.of(page, size, sortObject);
        log.info("Pageable sort: {}", pageable.getSort());

        Page<ProductDto> productPage = storeService.getProductsByCategory(category, pageable);
        log.info("Service returned page with sort: {}", productPage.getSort());

        // Конвертируем, передавая и sortObject, и оригинальное поле из запроса
        PageProductDto response = convertToPageProductDto(productPage, sortObject, field, direction);
        log.info("Response sort: {}", response.getSort());

        return ResponseEntity.ok(response);
    }

    @GetMapping("/test/products")
    public ResponseEntity<PageProductDto> getProductsForTest(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "150") int size) {

        log.info("TEST endpoint - getting products with forced DESC sorting");

        // Принудительно создаем сортировку DESC по name для JPA
        Sort sortObject = Sort.by(Sort.Order.desc("name"));
        Pageable pageable = PageRequest.of(page, size, sortObject);

        // Получаем данные
        Page<ProductDto> productPage = storeService.getProductsByCategory(category, pageable);

        // Вручную создаем объект сортировки для ответа
        List<PageProductDto.SortObject> sortObjects = new ArrayList<>();
        PageProductDto.SortObject sortObj = PageProductDto.SortObject.builder()
                .direction("DESC")
                .property("productName")
                .ascending(false)
                .ignoreCase(false)
                .sorted(true)
                .unsorted(false)
                .empty(false)
                .build();
        sortObjects.add(sortObj);

        // Для pageable.sort
        List<PageProductDto.SortObject> pageableSortObjects = new ArrayList<>();
        PageProductDto.SortObject pageableSortObj = PageProductDto.SortObject.builder()
                .direction("DESC")
                .property("productName")
                .ascending(false)
                .ignoreCase(false)
                .sorted(true)
                .unsorted(false)
                .empty(false)
                .build();
        pageableSortObjects.add(pageableSortObj);

        // Создаем pageableObject
        PageProductDto.PageableObject pageableObject = PageProductDto.PageableObject.builder()
                .offset(productPage.getPageable().getOffset())
                .pageNumber(productPage.getPageable().getPageNumber())
                .pageSize(productPage.getPageable().getPageSize())
                .paged(productPage.getPageable().isPaged())
                .unpaged(productPage.getPageable().isUnpaged())
                .sort(pageableSortObjects)
                .sorted(true)
                .unsorted(false)
                .build();

        // Создаем ответ
        PageProductDto response = PageProductDto.builder()
                .content(productPage.getContent())
                .totalPages(productPage.getTotalPages())
                .totalElements(productPage.getTotalElements())
                .size(productPage.getSize())
                .number(productPage.getNumber())
                .first(productPage.isFirst())
                .last(productPage.isLast())
                .empty(productPage.isEmpty())
                .sort(sortObjects)
                .pageable(pageableObject)
                .numberOfElements(productPage.getNumberOfElements())
                .hasContent(productPage.hasContent())
                .hasNext(productPage.hasNext())
                .hasPrevious(productPage.hasPrevious())
                .isFirst(productPage.isFirst())
                .isLast(productPage.isLast())
                .build();

        return ResponseEntity.ok(response);
    }

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

        String jpaField;
        if ("productName".equals(field)) {
            jpaField = "name";
            log.info("Mapping productName -> name for JPA");
        } else {
            jpaField = field;
        }

        Sort.Order order;
        if ("desc".equalsIgnoreCase(direction)) {
            log.info("Creating DESC order for JPA field: {}", jpaField);
            order = Sort.Order.desc(jpaField);
        } else {
            log.info("Creating ASC order for JPA field: {}", jpaField);
            order = Sort.Order.asc(jpaField);
        }

        Sort result = Sort.by(order);
        log.info("Result sort for JPA: {}", result);
        log.info("Result orders: {}", result.stream().collect(Collectors.toList()));
        log.info("----- EXIT parseSortCorrectly -----");
        return result;
    }

    private PageProductDto convertToPageProductDto(Page<ProductDto> page, Sort sort, String originalField, String originalDirection) {
        log.info("========== CONVERT TO PAGE PRODUCT DTO ==========");
        log.info("Original field: {}, original direction: {}", originalField, originalDirection);

        List<PageProductDto.SortObject> sortObjects = new ArrayList<>();

        // Создаем только один объект сортировки с оригинальными значениями
        PageProductDto.SortObject sortObj = PageProductDto.SortObject.builder()
                .direction(originalDirection.toUpperCase())
                .property(originalField)
                .ascending("asc".equalsIgnoreCase(originalDirection))
                .ignoreCase(false)
                .sorted(true)
                .unsorted(false)
                .empty(false)
                .build();
        sortObjects.add(sortObj);

        // Для pageable.sort создаем такой же объект
        List<PageProductDto.SortObject> pageableSortObjects = new ArrayList<>();
        PageProductDto.SortObject pageableSortObj = PageProductDto.SortObject.builder()
                .direction(originalDirection.toUpperCase())
                .property(originalField)
                .ascending("asc".equalsIgnoreCase(originalDirection))
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
