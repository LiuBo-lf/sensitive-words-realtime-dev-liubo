package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @ Package com.retailersv.func.IntervalJoinOrderCommentAndOrderInfoFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:42
 * @ description:
 * @ version 1.0
 */
/**
 * 定义一个用于区间连接OrderComment和OrderInfo的处理函数类
 * 该类继承自ProcessJoinFunction，用于处理两个数据流的连接操作
 * 具体功能是将OrderInfo中的字段添加到OrderComment中，以实现数据的富化
 * @author 26434
 */
public class IntervalJoinOrderCommentAndOrderInfoFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {

    /**
     * 处理元素的方法，将info中的字段添加到comment中，并输出合并后的结果
     *
     * @param comment 订单评论的JSON对象，作为连接的左侧输入
     * @param info 订单信息的JSON对象，作为连接的右侧输入
     * @param ctx 上下文对象，提供访问函数运行时信息的能力
     * @param out 用于输出处理后的元素到下游的收集器
     */
    @Override
    public void processElement(JSONObject comment, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out){
        // 克隆comment对象，以避免修改原始数据
        JSONObject enrichedComment = (JSONObject)comment.clone();

        // 遍历info对象的所有键，将info中的值添加到enrichedComment中
        // 键名前加上"info_"前缀，以区分原始comment中的字段
        for (String key : info.keySet()) {
            enrichedComment.put("info_" + key, info.get(key));
        }

        // 将富化后的评论信息输出到下游
        out.collect(enrichedComment);
    }
}

