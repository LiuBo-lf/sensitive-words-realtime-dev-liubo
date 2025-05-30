package com.lb.stream.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @ Package com.lb.stream.realtime.function.BeanToJsonStrMapFunction
 * @ Author  liu.bo
 * @ Date  2025/5/14 22:03
 * @ description:
 * @ version 1.0
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction <T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}

