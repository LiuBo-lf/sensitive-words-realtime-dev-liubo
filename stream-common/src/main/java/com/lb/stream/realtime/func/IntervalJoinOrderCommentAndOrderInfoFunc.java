package com.lb.stream.realtime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
/**
 * @ Package com.lb.stream.realtime.func.IntervalJoinOrderCommentAndOrderInfoFunc
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:54
 * @ description:orderComment Join orderInfo Msg 订单联合
 * @ version 1.0
 */
public class IntervalJoinOrderCommentAndOrderInfoFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject comment, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out){
        JSONObject enrichedComment = (JSONObject)comment.clone();

        for (String key : info.keySet()) {
            enrichedComment.put("info_" + key, info.get(key));
        }
        out.collect(enrichedComment);
    }
}
