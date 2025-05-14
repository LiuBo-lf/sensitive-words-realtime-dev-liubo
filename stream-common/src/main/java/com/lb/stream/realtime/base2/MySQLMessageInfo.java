package com.lb.stream.realtime.base2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Package com.lb.stream.realtime.base2.MySQLMessageInfo
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:45
 * @ description:
 * @ version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class MySQLMessageInfo {
    private String id;
    private String op;
    private String db_name;
    private String log_before;
    private String log_after;
    private String t_name;
    private String ts;
}
