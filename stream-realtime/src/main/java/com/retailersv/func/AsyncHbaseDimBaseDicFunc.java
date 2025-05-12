package com.retailersv.func;

import avro.shaded.com.google.common.cache.Cache;
import avro.shaded.com.google.common.cache.CacheBuilder;
import com.alibaba.fastjson.JSONObject;
import com.retailersv.util.HbaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @ Package com.retailersv.func.AsyncHbaseDimBaseDicFunc
 * @ Author  liu.bo
 * @ Date  2025/5/12 16:40
 * @ description:
 * @ version 1.0
 */
/**
 * AsyncHbaseDimBaseDicFunc类继承了RichAsyncFunction，用于异步查询HBase中的维度基础字典数据
 * 它通过缓存和HBase连接来实现对字典名称的快速查询和富化
 */
public class AsyncHbaseDimBaseDicFunc extends RichAsyncFunction<JSONObject,JSONObject> {

    // HBase连接
    private transient Connection hbaseConn;
    // 维度表
    private transient Table dimTable;
    // 缓存：RowKey -> dic_name，用于缓存查询过的字典名称
    private transient Cache<String, String> cache;

    /**
     * 初始化资源，包括HBase连接、维度表和缓存
     * @param parameters 配置参数
     * @throws Exception 如果初始化失败
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化HBase连接
        hbaseConn = new HbaseUtils("192.168.200.140:2181,192.168.200.141:2181,192.168.200.142:2181").getConnection();
        // 获取维度表
        dimTable = hbaseConn.getTable(TableName.valueOf("realtime_v2:dim_base_dic"));
        // 初始化缓存
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        super.open(parameters);
    }

    /**
     * 异步调用方法，用于查询和富化输入的JSONObject
     * @param input 输入的JSONObject，包含待查询的appraise字段
     * @param resultFuture 用于完成查询结果的Future
     * @throws Exception 如果查询失败
     */
    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        // 获取appraise字段值
        String appraise = input.getJSONObject("after").getString("appraise");
        // 计算appraise的MD5值作为RowKey
        String rowKey = MD5Hash.getMD5AsHex(appraise.getBytes(StandardCharsets.UTF_8));
        // 从缓存中获取字典名称
        String cachedDicName = cache.getIfPresent(rowKey);
        if (cachedDicName != null) {
            // 如果缓存中存在，则直接富化并发射结果
            enrichAndEmit(input, cachedDicName, resultFuture);
        }
        // 异步查询HBase
        CompletableFuture.supplyAsync(() -> {
            Get get = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
            try {
                // 执行查询
                Result result = dimTable.get(get);
                if (result.isEmpty()) {
                    // 如果结果为空，则返回null
                    return null;
                }
                // 返回查询到的字典名称
                return Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dic_name")));
            } catch (IOException e) {
                // 如果查询失败，则抛出异常
                throw new RuntimeException("Class: AsyncHbaseDimBaseDicFunc Line 66 HBase query failed ! ! !",e);
            }
        }).thenAccept(dicName -> {
            // 处理查询结果
            if (dicName != null) {
                // 如果查询到字典名称，则存入缓存并富化发射结果
                cache.put(rowKey, dicName);
                enrichAndEmit(input, dicName, resultFuture);
            }else {
                // 如果未查询到，则富化发射结果为"N/A"
                enrichAndEmit(input, "N/A", resultFuture);
            }
        });
    }

    /**
     * 富化输入的JSONObject并发射结果
     * @param input 输入的JSONObject
     * @param dicName 查询到的字典名称
     * @param resultFuture 用于完成查询结果的Future
     */
    private void enrichAndEmit(JSONObject input, String dicName, ResultFuture<JSONObject> resultFuture) {
        // 富化：将字典名称放入JSONObject的after字段中
        JSONObject after = input.getJSONObject("after");
        after.put("dic_name", dicName);
        // 完成结果
        resultFuture.complete(Collections.singleton(input));
    }

    /**
     * 处理查询超时
     * @param input 输入的JSONObject
     * @param resultFuture 用于完成查询结果的Future
     * @throws Exception 如果处理超时失败
     */
    @Override
    public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }

    /**
     * 关闭资源，包括维度表和HBase连接
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        try {
            // 关闭维度表
            if (dimTable != null) {
                dimTable.close();
            }
            // 关闭HBase连接
            if (hbaseConn != null) {
                hbaseConn.close();
            }
        } catch (Exception e) {
            // 如果关闭失败，则打印异常信息
            e.printStackTrace();
        }
        super.close();
    }
}

