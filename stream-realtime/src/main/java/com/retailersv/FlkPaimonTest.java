package com.retailersv;

import com.retailersv.stream.utils.PaimonMinioUtils;
import com.retailersv.util.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ Package com.retailersv.FlkPaimonTest
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:32
 * @ description:
 * @ version 1.0
 */
public class FlkPaimonTest {

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "NONE");

        PaimonMinioUtils.ExecCreateMinioCatalogAndDatabases(tenv,"minio_paimon_catalog","realtime_v2");


        tenv.executeSql("select * from realtime_v2.res_cart_info_tle where ds in (20250507,20250506) ").print();

    }
}
