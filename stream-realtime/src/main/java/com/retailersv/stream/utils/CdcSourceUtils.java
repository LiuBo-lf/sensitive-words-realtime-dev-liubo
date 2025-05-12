package com.retailersv.stream.utils;

import com.retailersv.util.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.util.Properties;

/**
 * @ Package com.retailersv.CdcSourceUtils
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:50
 * @ description:
 * @ version 1.0
 */
/**
 * CdcSourceUtils类提供了用于构建MySQL CDC（变更数据捕获）源的工具方法
 * CDC是一种用于捕获并跟踪数据库更改的技术，以便将这些更改传播到其他系统
 */
public class CdcSourceUtils {

    /**
     * 构建一个MySQL CDC源实例
     *
     * @param database 数据库名称，用于指定监听变更的数据库
     * @param table 表名称，用于指定监听变更的具体表
     * @param username 数据库用户名，用于连接数据库
     * @param pwd 数据库密码，用于连接数据库
     * @param model 启动选项，定义了从哪个时间点开始捕获变更
     * @return 返回一个配置好的MySqlSource实例，用于捕获MySQL数据库的变更
     */
    public static MySqlSource<String> getMySQLCdcSource(String database, String table, String username, String pwd, StartupOptions model){
        // 配置Debezium属性，Debezium是一个开源的CDC工具
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8"); // 设置数据库连接字符集
        debeziumProperties.setProperty("decimal.handling.mode","string"); // 设置decimal类型处理模式为字符串
        debeziumProperties.setProperty("time.precision.mode","connect"); // 设置时间精度模式为Connect兼容模式

        // 构建MySqlSource实例
        return MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host")) // 设置MySQL主机名
                .port(ConfigUtils.getInt("mysql.port")) // 设置MySQL端口号
                .databaseList(database) // 设置监听变更的数据库列表
                .tableList(table) // 设置监听变更的表列表
                .username(username) // 设置数据库用户名
                .password(pwd) // 设置数据库密码
                .serverTimeZone(ConfigUtils.getString("mysql.timezone")) // 设置服务器时区
                .deserializer(new JsonDebeziumDeserializationSchema()) // 设置反序列化器
                .startupOptions(model) // 设置启动选项，决定从哪个时间点开始捕获变更
                .includeSchemaChanges(true) // 包含架构变更，表示也要捕获数据库架构的更改
                .debeziumProperties(debeziumProperties) // 设置Debezium属性
                .build(); // 构建并返回配置好的MySqlSource实例
    }
}

