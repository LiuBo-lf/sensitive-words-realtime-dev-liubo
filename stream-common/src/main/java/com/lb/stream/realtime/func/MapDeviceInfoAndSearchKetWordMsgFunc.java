package com.lb.stream.realtime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
/**
 * @ Package com.lb.stream.realtime.func.MapDeviceInfoAndSearchKetWordMsgFunc
 * @ Author  liu.bo
 * @ Date  2025/5/14 21:56
 * @ description:page log设备信息处理
 * @ version 1.0
 */
public class MapDeviceInfoAndSearchKetWordMsgFunc extends RichMapFunction<JSONObject,JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        if (jsonObject.containsKey("common")){
            JSONObject common = jsonObject.getJSONObject("common");
            result.put("uid",common.getString("uid") != null ? common.getString("uid") : "-1");
            result.put("ts",jsonObject.getLongValue("ts"));
            JSONObject deviceInfo = new JSONObject();
            common.remove("sid");
            common.remove("mid");
            common.remove("is_new");
            deviceInfo.putAll(common);
            result.put("deviceInfo",deviceInfo);
            if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                JSONObject pageInfo = jsonObject.getJSONObject("page");
                if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                    String item = pageInfo.getString("item");
                    result.put("search_item",item);
                }
            }
        }
        JSONObject deviceInfo = result.getJSONObject("deviceInfo");
        String os = deviceInfo.getString("os").split(" ")[0];
        deviceInfo.put("os",os);


        return result;
    }
}
