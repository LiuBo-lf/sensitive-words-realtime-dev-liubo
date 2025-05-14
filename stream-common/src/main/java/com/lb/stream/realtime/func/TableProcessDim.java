package com.lb.stream.realtime.func;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @ Package com.lb.stream.realtime.func.TableProcessDim
 * @ Author  liu.bo
 * @ Date  2025/5/14 22:00
 * @ description:
 * @ version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim implements Serializable {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    String sinkRowKey;

    String op;
}
