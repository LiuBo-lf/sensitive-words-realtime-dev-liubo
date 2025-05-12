package com.retailersv.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ Package com.retailersv.func.ProcessSplitStreamFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:43
 * @ description:
 * @ version 1.0
 */
/**
 * ProcessSplitStreamFunc是一个继承自ProcessFunction的类，用于处理JSON对象并将其拆分成不同的流
 * 它根据JSON对象中的内容将其输出到不同的输出标签下，以便后续处理
 */
public class ProcessSplitStreamFunc extends ProcessFunction<JSONObject,String> {

    //定义错误信息的输出标签
    private OutputTag<String> errTag;
    //定义开始信息的输出标签
    private OutputTag<String> startTag ;
    //定义展示信息的输出标签
    private OutputTag<String> displayTag ;
    //定义动作信息的输出标签
    private OutputTag<String> actionTag ;

    /**
     * 构造函数，初始化输出标签
     * @param errTag 错误信息输出标签
     * @param startTag 开始信息输出标签
     * @param displayTag 展示信息输出标签
     * @param actionTag 动作信息输出标签
     */
    public ProcessSplitStreamFunc(OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        this.startTag = startTag;
        this.errTag = errTag;
        this.actionTag = actionTag;
        this.displayTag = displayTag;
    }

    /**
     * 处理元素函数，根据JSON对象中的内容将其输出到不同的流
     * @param jsonObject 输入的JSON对象
     * @param context 上下文，用于输出到侧流
     * @param collector 收集器，用于输出到主流
     * @throws Exception 如果处理过程中发生错误
     */
    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
        //处理错误信息，如果存在，则输出到错误信息流，并从原JSON对象中移除
        JSONObject errJson = jsonObject.getJSONObject("err");
        if (errJson != null){
            context.output(errTag, errJson.toJSONString());
            jsonObject.remove("err");
        }
        //处理开始信息，如果存在，则输出到开始信息流
        JSONObject startJsonObj = jsonObject.getJSONObject("start");
        if (startJsonObj != null) {
            context.output(startTag, jsonObject.toJSONString());
        }else {
            //处理展示和动作信息
            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
            Long ts = jsonObject.getLong("ts");
            JSONArray displayArr = jsonObject.getJSONArray("displays");
            if (displayArr != null && displayArr.size() > 0) {
                //遍历当前页面的所有曝光信息
                for (int i = 0; i < displayArr.size(); i++) {
                    JSONObject disPlayJsonObj = displayArr.getJSONObject(i);
                    //定义一个新的JSON对象，用于封装遍历出来的曝光数据
                    JSONObject newDisplayJsonObj = new JSONObject();
                    newDisplayJsonObj.put("common", commonJsonObj);
                    newDisplayJsonObj.put("page", pageJsonObj);
                    newDisplayJsonObj.put("display", disPlayJsonObj);
                    newDisplayJsonObj.put("ts", ts);
                    //将曝光日志写到曝光侧输出流
                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                }
                jsonObject.remove("displays");
            }
            JSONArray actionArr = jsonObject.getJSONArray("actions");
            if (actionArr != null && actionArr.size() > 0) {
                //遍历出每一个动作
                for (int i = 0; i < actionArr.size(); i++) {
                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                    //定义一个新的JSON对象，用于封装动作信息
                    JSONObject newActionJsonObj = new JSONObject();
                    newActionJsonObj.put("common", commonJsonObj);
                    newActionJsonObj.put("page", pageJsonObj);
                    newActionJsonObj.put("action", actionJsonObj);
                    //将动作日志写到动作侧输出流
                    context.output(actionTag, newActionJsonObj.toJSONString());
                }
                jsonObject.remove("actions");
            }
            //收集处理后的JSON对象到主流
            collector.collect(jsonObject.toJSONString());
        }
    }
}

