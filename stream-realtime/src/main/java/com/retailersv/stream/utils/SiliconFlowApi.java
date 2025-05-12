package com.retailersv.stream.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.retailersv.util.ConfigUtils;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @ Package com.retailersv.stream.utils.SiliconFlowApi
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:55
 * @ description:
 * @ version 1.0
 */
/**
 * SiliconFlow API 类，用于与SiliconFlow API进行交互
 */
public class SiliconFlowApi {
    // 静态初始化块，配置JSON生成特性，禁用循环引用检测
    static {
        JSONObject.DEFAULT_GENERATE_FEATURE |= SerializerFeature.DisableCircularReferenceDetect.getMask();
    }

    // 连接池配置，设置最大连接数、空闲连接数和空闲时间
    private static final ConnectionPool CONNECTION_POOL = new ConnectionPool(200, 5, TimeUnit.MINUTES);
    // API地址常量
    private static final String SILICON_API_ADDR = "https://api.siliconflow.cn/v1/chat/completions";
    // 从配置文件中读取API token
    private static final String SILICON_API_TOKEN = ConfigUtils.getString("silicon.api.token");
    // OkHttpClient配置，使用连接池，设置连接超时、读取超时和重连策略
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectionPool(CONNECTION_POOL)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

    /**
     * 生成差评文本
     *
     * @param prompt 提示词，用于生成差评的输入
     * @param apiToken API token，用于认证
     * @return 生成的差评文本或错误信息
     */
    public static String generateBadReview(String prompt, String apiToken) {
        try {
            // 构建请求体
            JSONObject requestBody = buildRequestBody(prompt);
            // 确保线程安全
            Thread.sleep(1);
            // 构建请求
            Request request = new Request.Builder()
                    .url(SILICON_API_ADDR)
                    .post(RequestBody.create(
                            MediaType.parse("application/json; charset=utf-8"),
                            requestBody.toJSONString()
                    ))
                    .addHeader("Authorization", "Bearer " + apiToken)
                    .addHeader("Content-Type", "application/json")
                    .build();

            // 执行请求并处理响应
            try (Response response = CLIENT.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    // 处理错误响应
                    handleError(response);
                    return "请求失败: HTTP " + response.code();
                }
                // 处理成功响应
                return processResponse(response);
            }
        } catch (IOException e) {
            // 处理IO异常
            handleException(e);
            return "网络异常: " + e.getMessage();
        } catch (Exception e) {
            // 处理其他异常
            return "系统错误: " + e.getMessage();
        }
    }

    /**
     * 构建请求体
     *
     * @param prompt 提示词
     * @return 请求体JSON对象
     */
    private static JSONObject buildRequestBody(String prompt) {
        // 构建请求体，设置模型参数
        return new JSONObject()
                .fluentPut("model", "Pro/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B")
                .fluentPut("stream", false)
                .fluentPut("max_tokens", 512)
                .fluentPut("temperature", 0.7)
                .fluentPut("top_p", 0.7)
                .fluentPut("top_k", 50)
                .fluentPut("frequency_penalty", 0.5)
                .fluentPut("n", 1)
                .fluentPut("messages", new JSONObject[]{
                        new JSONObject()
                                .fluentPut("role", "user")
                                .fluentPut("content", prompt)
                });
    }

    /**
     * 处理成功响应
     *
     * @param response 响应对象
     * @return 生成的差评文本
     * @throws IOException 如果响应体为空，则抛出IO异常
     */
    private static String processResponse(Response response) throws IOException {
        if (response.body() == null) {
            throw new IOException("空响应体");
        }

        String responseBody = response.body().string();
        JSONObject result = JSON.parseObject(responseBody);

        // 验证响应结构
        if (!result.containsKey("choices")) {
            throw new RuntimeException("无效响应结构: 缺少choices字段");
        }

        if (result.getJSONArray("choices").isEmpty()) {
            throw new RuntimeException("响应内容为空");
        }

        // 提取并返回差评文本
        return result.getJSONArray("choices")
                .getJSONObject(0)
                .getJSONObject("message")
                .getString("content");
    }

    /**
     * 处理错误响应
     *
     * @param response 错误响应对象
     */
    private static void handleError(Response response) throws IOException {
        String errorBody = response.body() != null ?
                response.body().string() : "无错误详情";
        System.err.println("API错误 [" + response.code() + "]: " + errorBody);
    }

    /**
     * 处理IO异常
     *
     * @param e IO异常对象
     */
    private static void handleException(IOException e) {
        System.err.println("网络异常: " + e.getMessage());
        e.printStackTrace();
    }

    // 主函数，用于测试
    public static void main(String[] args) {
        // 测试用例（需要有效token）
        String result = generateBadReview(
                "给出一个电商差评，攻击性拉满，使用脏话，20字数以内，不需要思考过程",
                SILICON_API_TOKEN
        );
        System.out.println("生成结果: " + result);
    }
}

