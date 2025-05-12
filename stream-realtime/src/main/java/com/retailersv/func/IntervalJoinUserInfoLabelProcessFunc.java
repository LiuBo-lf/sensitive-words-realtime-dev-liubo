package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @ Package com.retailersv.func.IntervalJoinUserInfoLabelProcessFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 22:21
 * @ description:
 * @ version 1.0
 */
public class IntervalJoinUserInfoLabelProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        if (jsonObject1.getLongValue("uid") == (jsonObject2.getLongValue("uid"))) {
            // 创建一个新的 JSONObject 用于存储合并后的数据
            JSONObject mergedJson = new JSONObject();

            // 将第一条流的数据添加到合并后的 JSONObject 中
            mergedJson.putAll(jsonObject2);

            // 补充第二条流中的维度信息
            mergedJson.put("unit_height", jsonObject1.getString("unit_height"));
            mergedJson.put("weight", jsonObject1.getString("weight"));
            mergedJson.put("unit_weight", jsonObject1.getString("unit_weight"));
            mergedJson.put("height", jsonObject1.getString("height"));

            // 将合并后的 JSONObject 发送到下游
            collector.collect(mergedJson);
        }
    }
}
