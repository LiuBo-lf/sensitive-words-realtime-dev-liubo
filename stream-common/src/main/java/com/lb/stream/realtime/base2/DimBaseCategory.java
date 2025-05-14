package com.lb.stream.realtime.base2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @ Package com.lb.stream.realtime.base2.DimBaseCategory
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:44
 * @ description:
 * @ version 1.0
 * @author 26434
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;

}
