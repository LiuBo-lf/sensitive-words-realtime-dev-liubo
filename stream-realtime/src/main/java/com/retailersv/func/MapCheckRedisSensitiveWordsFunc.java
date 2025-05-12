package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.util.RedisLuaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @ Package com.retailersv.func.MapCheckRedisSensitiveWordsFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:42
 * @ description:
 * @ version 1.0
 */
/**
 * 该类用于检查评论中的敏感词是否违反规定
 * 它继承自RichMapFunction，用于处理流数据中的每个元素
 * 主要功能是检查评论文本中的最后一个词是否为敏感词，并设置相应的违规等级和信息
 */
public class MapCheckRedisSensitiveWordsFunc extends RichMapFunction<JSONObject,JSONObject> {

    /**
     * 主要逻辑函数，用于处理每个JSON对象
     * 该函数会检查评论文本中的最后一个词是否为敏感词，并在结果中设置违规信息
     *
     * @param jsonObject 输入的JSON对象，包含用户评论信息
     * @return 处理后的JSON对象，包含违规检查结果
     * @throws Exception 如果在处理过程中发生错误，抛出异常
     */
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // 初始化结果JSON对象
        JSONObject resultJson = new JSONObject();
        // 公共字段提取到外部，避免重复设置
        resultJson.put("user_id", jsonObject.getLong("user_id"));
        resultJson.put("consignee", jsonObject.getString("info_consignee"));
        resultJson.put("ts_ms",jsonObject.getLong("ts_ms"));
        resultJson.put("ds",jsonObject.getString("ds"));

        // 获取评论文本并拆分单词
        String commentTxt = jsonObject.getString("commentTxt");
        String[] words = commentTxt.split(",");
        // 保留原始评论文本
        resultJson.put("msg", commentTxt);
        // 获取最后一个单词，如果有的话
        String lastWord = words.length > 0 ? words[words.length - 1] : "";

        // 检查最后一个单词是否违反规定
        boolean isViolation = RedisLuaUtils.checkSingle(lastWord);
        // 设置是否违反规定的标志
        resultJson.put("is_violation", isViolation ? 1 : 0);

        // 根据检查结果设置违规等级和消息
        if (isViolation) {
            // 违规时设置违规相关字段
            resultJson.put("violation_grade", "P0");
            resultJson.put("violation_msg", lastWord);
        } else {
            // 非违规时设置默认值和额外信息
            resultJson.put("violation_grade", "");
            resultJson.put("violation_msg", "");
        }

        // 返回处理结果
        return resultJson;
    }
}

