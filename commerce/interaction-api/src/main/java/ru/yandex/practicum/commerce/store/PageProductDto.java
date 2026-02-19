package ru.yandex.practicum.commerce.store;

import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.commerce.dto.ProductDto;

import java.util.List;

@Data
@Builder
public class PageProductDto {
    private List<ProductDto> content;
    private int totalPages;
    private long totalElements;
    private int size;
    private int number;
    private boolean first;
    private boolean last;
    private boolean empty;
    private SortObject sort;
    private PageableObject pageable;

    @Data
    @Builder
    public static class SortObject {
        private String direction;
        private String property;
        private boolean ascending;
        private boolean ignoreCase;
    }

    @Data
    @Builder
    public static class PageableObject {
        private long offset;
        private int pageNumber;
        private int pageSize;
        private boolean paged;
        private boolean unpaged;
        private List<SortObject> sort;
    }
}
