package com.retailersv.catalog.ddl.hbase;

import com.retailersv.util.ConfigUtils;
import com.retailersv.util.HiveCatalogUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ Package com.retailersv.catalog.ddl.hbase.CreateHbaseDimDDLCataLog
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:22
 * @ description:Hbase Dim Catalog
 * @ version 1.0
 * @author 26434
 */
public class CreateHbaseDimDDLCataLog {
    // 定义HBase命名空间常量
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    // 定义Zookeeper服务器主机列表常量
    private static final String ZOOKEEPER_SERVER_HOST_LIST = ConfigUtils.getString("zookeeper.server.host.list");
    // 定义HBase连接版本常量
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";
    // 定义删除表的前缀语句常量
    private static final String DROP_TABEL_PREFIX = "drop table if exists ";

    // 定义创建HBase维度基础字典表的DDL语句
    private static final String createHbaseDimBaseDicDDL =
            "create table hbase_dim_base_dic (" +
                    "    rk string," +
                    "    info row<dic_name string, parent_code string>," +
                    "    primary key (rk) not enforced" +
                    ")" +
                    "with (" +
                    "    'connector' = '" + HBASE_CONNECTION_VERSION + "'," +
                    "    'table-name' = '" + HBASE_NAME_SPACE + ":dim_base_dic'," +
                    "    'zookeeper.quorum' = '" + ZOOKEEPER_SERVER_HOST_LIST + "'" +
                    ")";

    /**
     * 主函数入口
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        // 设置Hadoop用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        // 初始化流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 初始化流表环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 获取Hive目录并注册到表环境
        HiveCatalog hiveCatalog = HiveCatalogUtils.getHiveCatalog("hive-catalog");
        tenv.registerCatalog("hive-catalog", hiveCatalog);
        tenv.useCatalog("hive-catalog");
        // 执行SQL显示当前所有的表
        tenv.executeSql("show tables;").print();
        // 执行SQL删除表
        tenv.executeSql(DROP_TABEL_PREFIX + getCreateTableDDLTableName(createHbaseDimBaseDicDDL));
        // 再次执行SQL显示当前所有的表
        tenv.executeSql("show tables;").print();
        // 执行SQL创建表
        tenv.executeSql(createHbaseDimBaseDicDDL).print();
        // 再次执行SQL显示当前所有的表
        tenv.executeSql("show tables;").print();
        // 执行SQL查询新创建表的数据
        tenv.executeSql("select * from hbase_dim_base_dic").print();
    }

    /**
     * 从创建表的DDL语句中提取表名
     * @param createDDL 创建表的DDL语句
     * @return 表名
     */
    public static String getCreateTableDDLTableName(String createDDL) {
        // 根据空格分割DDL语句并返回表名
        return createDDL.split(" ")[2].trim();
    }
}
