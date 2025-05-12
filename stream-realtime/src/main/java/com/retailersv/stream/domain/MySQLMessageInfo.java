package com.retailersv.stream.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Package com.retailersv.stream.domain.MySQLMessageInfo
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:45
 * @ description:binlog
 * @ version 1.0
 */
/**
 * MySQLMessageInfo类用于封装MySQL中一条变更消息的详细信息
 * 这个类使用了Lombok注解来简化构造函数和getter、setter的编写
 */
@AllArgsConstructor // Lombok注解，自动生成包含所有字段的构造函数
@NoArgsConstructor  // Lombok注解，自动生成无参构造函数
@Data            // Lombok注解，自动生成getter和setter方法
public class MySQLMessageInfo {
    private String id;      // 数据库中变更消息的唯一标识符
    private String op;      // 变更操作类型，例如插入、更新或删除
    private String db_name; // 发生变更的数据库名称
    private String log_before; // 变更前的数据快照，仅在更新或删除操作时有意义
    private String log_after;  // 变更后的数据快照，仅在插入或更新操作时有意义
    private String t_name;    // 发生变更的表名称
    private String ts;        // 变更的时间戳，表示变更发生的时间
}
