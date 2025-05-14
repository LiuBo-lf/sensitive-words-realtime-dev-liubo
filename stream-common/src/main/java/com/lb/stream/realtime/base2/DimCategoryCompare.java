package com.lb.stream.realtime.base2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Package com.lb.stream.realtime.base2.DimCategoryCompare
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:45
 * @ description:
 * @ version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
