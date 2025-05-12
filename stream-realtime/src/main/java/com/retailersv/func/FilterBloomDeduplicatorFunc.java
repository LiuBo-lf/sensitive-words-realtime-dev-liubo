package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @ Package com.retailersv.func.FilterBloomDeduplicatorFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:41
 * @ description:
 * @ version 1.0
 */
/**
 * 使用布隆过滤器进行去重的过滤函数
 * 该类主要用于在流处理过程中过滤掉重复的数据
 * 它利用布隆过滤器数据结构，在保证高效率的同时，以较低的代价检测数据是否已经处理过
 */
public class FilterBloomDeduplicatorFunc extends RichFilterFunction<JSONObject> {

    // 日志记录器，用于记录运行时信息
    private static final Logger logger = LoggerFactory.getLogger(FilterBloomDeduplicatorFunc.class);

    // 期望插入的元素数量，用于初始化布隆过滤器
    private final int expectedInsertions;
    // 可接受的假阳性率，用于初始化布隆过滤器
    private final double falsePositiveRate;
    // 布隆过滤器的状态，存储位数组
    private transient ValueState<byte[]> bloomState;

    /**
     * 构造函数
     *
     * @param expectedInsertions 期望插入的元素数量
     * @param falsePositiveRate 可接受的假阳性率
     */
    public FilterBloomDeduplicatorFunc(int expectedInsertions, double falsePositiveRate) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
    }

    /**
     * 初始化函数，用于设置布隆过滤器的状态
     *
     * @param parameters 配置参数
     */
    @Override
    public void open(Configuration parameters){
        // 描述符，定义状态名称和类型
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "bloomFilterState",
                BytePrimitiveArraySerializer.INSTANCE
        );

        // 获取运行时上下文，并初始化状态
        bloomState = getRuntimeContext().getState(descriptor);
    }

    /**
     * 过滤函数，用于判断数据是否已经存在
     *
     * @param value 输入的JSON对象
     * @return 如果数据可能重复，则返回false；如果是新数据，则返回true
     * @throws Exception 如果处理过程中发生错误
     */
    @Override
    public boolean filter(JSONObject value) throws Exception {
        // 提取订单ID和时间戳，组合成唯一键
        long orderId = value.getLong("order_id");
        long tsMs = value.getLong("ts_ms");
        String compositeKey = orderId + "_" + tsMs;

        // 读取状态
        byte[] bitArray = bloomState.value();
        if (bitArray == null) {
            // 如果状态为空，初始化位数组
            bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
        }

        // 假设数据可能已经存在
        boolean mightContain = true;
        // 计算哈希值
        int hash1 = hash(compositeKey);
        int hash2 = hash1 >>> 16;

        // 使用多个哈希函数检查位数组
        for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % (bitArray.length * 8);

            int bytePos = pos / 8;
            int bitPos = pos % 8;
            byte current = bitArray[bytePos];

            // 如果位数组中某位未设置，说明数据是新的
            if ((current & (1 << bitPos)) == 0) {
                mightContain = false;
                // 设置位数组中的对应位
                bitArray[bytePos] = (byte) (current | (1 << bitPos));
            }
        }

        // 如果是新数据，更新状态并保留
        if (!mightContain) {
            bloomState.update(bitArray);
            return true;
        }

        // 可能重复的数据，过滤
        logger.warn("check duplicate data : {}", value);
        return false;
    }

    /**
     * 计算最优哈希函数数量
     *
     * @param n 期望插入的元素数量
     * @param m 位数组的长度
     * @return 最优哈希函数数量
     */
    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * 计算最优位数组长度
     *
     * @param n 期望插入的元素数量
     * @param p 假阳性率
     * @return 最优位数组长度
     */
    private int optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 计算字符串的哈希值
     *
     * @param key 输入字符串
     * @return 哈希值
     */
    private int hash(String key) {
        return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asInt();
    }
}

