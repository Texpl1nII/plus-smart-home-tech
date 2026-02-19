package ru.yandex.practicum.commerce.store;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.commerce.dto.ProductDto;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
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

    private Integer numberOfElements;
    private Boolean hasContent;
    private Boolean hasNext;
    private Boolean hasPrevious;
    private Boolean isFirst;
    private Boolean isLast;

    @Data
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SortObject {
        private String direction;
        private String property;
        private boolean ascending;
        private boolean ignoreCase;

        private Boolean sorted;
        private Boolean unsorted;
        private Boolean empty;
    }

    @Data
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class PageableObject {
        private long offset;
        private int pageNumber;
        private int pageSize;
        private boolean paged;
        private boolean unpaged;
        private List<SortObject> sort;

        private Boolean sorted;
        private Boolean unsorted;
    }
}
