package com.retailersv.stream;

import com.retailersv.util.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ Package com.retailersv.stream.Test
 * @ Author  liu.bo
 * @ Date  2025/5/12 19:42
 * @ description: 
 * @ version 1.0
*/
public class Test {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.200.142", 15455);

        dataStreamSource.print();




        env.execute();
    }

}
