package com.lb.stream.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Package com.lb.stream.realtime.bean.TableProcessDim
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:48
 * @ description:
 * @ version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {

    String sourceTable;

    String sinkTable;

    String sinkColumns;

    String sinkFamily;

    String sinkRowKey;

    String op;

}
