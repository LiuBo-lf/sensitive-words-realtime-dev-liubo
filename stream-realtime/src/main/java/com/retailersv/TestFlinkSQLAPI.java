package com.retailersv;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ Package com.retailersv.TestFlinkSQLAPI
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:33
 * @ description:
 * @ version 1.0
 */
public class TestFlinkSQLAPI {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "/Users/zhouhan/dev_env/work_project/java/stream-dev/stream-realtime/src/main/resources");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");


        tenv.executeSql("select rk,\n" +
                "       info.dic_name as dic_name,\n" +
                "       info.parent_code as parent_code\n" +
                "from hbase_dim_base_dic").print();

    }
}
