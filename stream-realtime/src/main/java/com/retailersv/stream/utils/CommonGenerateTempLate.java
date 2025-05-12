package com.retailersv.stream.utils;

import com.retailersv.util.ConfigUtils;

import static com.retailersv.stream.utils.SiliconFlowApi.generateBadReview;

/**
 * @ Package com.retailersv.stream.utils.CommonGenerateTempLate
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:56
 * @ description:
 * @ version 1.0
 */
/**
 * 该类提供了生成临时评论模板的功能，主要用于电商环境中的商品评论
 */
public class CommonGenerateTempLate {

    // 定义评论模板，用于生成带有商品名称和特性的评论
    private static final String COMMENT_TEMPLATE = "生成一个电商%s,商品名称为%s,20字数以内,%s不需要思考过程 ";

    // 定义默认评论性质，此处为"差评"
    private static final String COMMENT = "差评";

    // 从配置中获取API令牌，用于调用生成评论的API
    private static final String API_TOKEN = ConfigUtils.getString("silicon.api.token");

    /**
     * 根据提供的评论性质和商品名称生成评论
     * 如果提供的评论性质为"差评"，则生成带有攻击性描述的评论，否则生成普通差评
     *
     * @param comment 评论性质，目前只处理"差评"情况
     * @param productName 商品名称，用于嵌入到评论中
     * @return 生成的评论字符串
     */
    public static String GenerateComment(String comment,String productName){
        // 检查是否为预定义的"差评"
        if (comment.equals(COMMENT)){
            // 是"差评"，则使用带有攻击性描述的模板生成评论
            return generateBadReview(
                    String.format(COMMENT_TEMPLATE, COMMENT, productName, "攻击性拉满,使用脏话"),
                    API_TOKEN
            );
        }
        // 非"差评"情况，使用普通描述生成评论
        return generateBadReview(
                String.format(COMMENT_TEMPLATE, COMMENT, productName,""),
                API_TOKEN
        );
    }

}

